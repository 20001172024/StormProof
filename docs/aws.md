# StormProof on AWS

Three EC2 instances running k3s (a small Kubernetes), provisioned with
Terraform from `deploy/terraform`, with StormProof, Prometheus and Grafana
deployed from `deploy/k8s`. Everything here costs money while it runs, so
the plan is: **apply, run the demo and the benchmarks, destroy, the same
day.**

## Cost

Approximate on-demand prices in us-east-1, as of this writing. **Check the
[AWS Pricing Calculator](https://calculator.aws/) before you apply**;
prices change and differ by region.

| What | Count | Approx. per hour |
|---|---|---|
| t3.small instance (2 vCPU, 2 GB) | 3 | $0.062 |
| Public IPv4 address | 3 | $0.015 |
| gp3 disk, 20 GB | 3 | $0.007 |
| VPC, subnet, security group, key pair, budget | | free |
| **Total** | | **about $0.09/hour** |

That is about **$0.25 for a three-hour session**, about $2 a day, or about
$65 for a month if you forget to destroy it. Data transfer for a demo is a
few cents at most. There is deliberately no NAT gateway (about $32/month),
load balancer or Elastic IP. The cheapest instance that fits is t3.small:
t3.micro (1 GB) is too small for k3s plus StormProof plus monitoring.

Before you start, **set a budget alert**: either set `budget_email` in
`terraform.tfvars` (Terraform then creates an AWS Budget that emails you
at half the limit and when the month's forecast passes it), or create one
in the AWS console under Billing > Budgets.

## What you need

- An AWS account and credentials for it on your machine (`aws configure`
  or `AWS_PROFILE`). Use an IAM user or role, not the root user.
- Terraform 1.6 or later, kubectl, and an SSH key pair.
- The StormProof image public on the GitHub Container Registry: CI pushes
  `ghcr.io/20001172024/stormproof` on every merge to main. Make it public
  once under GitHub > Packages > stormproof > Package settings > Change
  visibility.

## 1. Create the cluster

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # set admin_cidr, ssh_public_key, budget_email
terraform init
terraform plan                                  # read it: 3 instances, a VPC, a security group, ...
terraform apply
```

It takes about two minutes for the instances, then two or three more for
k3s to install on them. Then, in another terminal, open a tunnel to the
Kubernetes API (nothing but SSH is open to the internet, and only from
your address):

```bash
$(terraform output -raw tunnel)                 # leave this running
```

and back in the first:

```bash
eval "$(terraform output -raw kubeconfig)"      # writes stormproof.kubeconfig
export KUBECONFIG=$PWD/stormproof.kubeconfig
kubectl get nodes                               # three nodes, Ready
```

## 2. Deploy StormProof

From the repository root:

```bash
kubectl apply -k deploy/k8s/overlays/aws
kubectl rollout status statefulset/stormproof
kubectl get pods -o wide                        # one server per node
kubectl port-forward svc/grafana 3000:3000      # http://localhost:3000: the StormProof dashboard
```

## 3. The chaos demo

With Grafana open:

```bash
deploy/k8s/chaos.sh
```

It starts the load generator as a Job, deletes the leader's pod, then
partitions the new leader with a NetworkPolicy for 20 s, printing how long
each failover took; at the end it prints the load generator's results.
Take the Grafana screenshot for the README while the partition is on.

## 4. Benchmarks

Edit the arguments in `deploy/k8s/jobs/bench.yaml` for each workload (for
example `-clients=32 -reads=0.1 -duration=20s`) and run it:

```bash
sed 's|image: stormproof$|image: ghcr.io/20001172024/stormproof:latest|' deploy/k8s/jobs/bench.yaml | kubectl apply -f -
kubectl wait --for=condition=Complete job/stormproof-bench --timeout=5m && kubectl logs job/stormproof-bench
kubectl delete job stormproof-bench
```

Record the instance type, region and k3s version with every result in
`docs/benchmarks.md`.

## 5. Destroy everything

```bash
kubectl delete -k deploy/k8s/overlays/aws      # optional: the instances are about to go anyway
cd deploy/terraform
terraform destroy                               # type "yes"
```

Then check nothing is left running (or billed):

```bash
aws ec2 describe-instances --filters Name=tag:Project,Values=stormproof \
  Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId' --output text   # should print nothing
aws ec2 describe-volumes --filters Name=tag:Project,Values=stormproof --query 'Volumes[].VolumeId' --output text
aws ec2 describe-vpcs --filters Name=tag:Project,Values=stormproof --query 'Vpcs[].VpcId' --output text
```

Every resource is tagged `Project=stormproof`, so the console's Tag
Editor (Resource Groups > Tag Editor, all regions) can also list anything
left over. The budget alert, if you created one, is removed by `terraform
destroy` too; keep your own account-wide alert anyway.

## Variant: EKS

The same manifests run on Amazon EKS, AWS's managed Kubernetes. It is not
the default because its control plane alone costs about $0.10 an hour
(about $73 a month), more than this whole k3s cluster, and the worker
nodes cost the same as here on top. To use it: create a cluster with
three t3.small nodes (with `eksctl create cluster --nodes 3
--node-type t3.small`, or the `terraform-aws-modules/eks/aws` module),
make sure the cluster has a default StorageClass (EKS needs the EBS CSI
driver add-on for the volumes), and `kubectl apply -k
deploy/k8s/overlays/aws`. The partition step of `chaos.sh` needs a network
plugin that enforces NetworkPolicy (on EKS, enable it in the VPC CNI).
Destroy with the same tool you created it with.

## What has and hasn't been tested

`terraform validate` passes with Terraform 1.17.2 and the AWS provider
6.73.0, and the instances' boot scripts render and pass a shell syntax
check. The Kubernetes objects were accepted by a k3s API server. None of
it has run on AWS yet: the first `terraform apply` is the real test, and
its results (and any fixes it needs) go into this document and
`docs/benchmarks.md`.
