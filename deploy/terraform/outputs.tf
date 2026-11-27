output "server_public_ip" {
  value = aws_instance.server.public_ip
}

output "agent_public_ips" {
  value = aws_instance.agent[*].public_ip
}

output "ssh" {
  description = "Log in to the k3s server."
  value       = "ssh ubuntu@${aws_instance.server.public_ip}"
}

output "kubeconfig" {
  description = "Fetch a kubeconfig for the tunnel below (it points at 127.0.0.1:6443)."
  value       = "ssh ubuntu@${aws_instance.server.public_ip} sudo cat /etc/rancher/k3s/k3s.yaml > stormproof.kubeconfig"
}

output "tunnel" {
  description = "Forward the Kubernetes API over SSH; leave it running, then export KUBECONFIG=$PWD/stormproof.kubeconfig."
  value       = "ssh -N -L 6443:127.0.0.1:6443 ubuntu@${aws_instance.server.public_ip}"
}
