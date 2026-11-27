# Three EC2 instances in one public subnet running k3s: one server (which
# also runs workloads) and two agents. Kept deliberately small and cheap:
# no NAT gateway, no load balancer, no Elastic IPs. The only way in is SSH
# from admin_cidr; the Kubernetes API is reached through an SSH tunnel (see
# outputs), so it is never exposed to the internet.

data "aws_availability_zones" "available" {
  state = "available"
}

# Ubuntu 24.04 LTS from Canonical's own account.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_vpc" "main" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "stormproof" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "stormproof" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.20.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "stormproof-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "stormproof-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "nodes" {
  name        = "stormproof-nodes"
  description = "StormProof k3s nodes: SSH from the admin only, anything between the nodes"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH from the admin"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }
  ingress {
    description = "Everything between the nodes (k3s API, flannel, kubelet, the servers' Raft traffic)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  egress {
    description = "Out to the internet: packages, k3s, container images"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "stormproof-nodes" }
}

resource "aws_key_pair" "admin" {
  key_name   = "stormproof-admin"
  public_key = var.ssh_public_key
}

# Shared secret for agents joining the server. It ends up in the Terraform
# state and the instances' user data, so keep the state file private (it is
# git-ignored).
resource "random_password" "k3s_token" {
  length  = 48
  special = false
}

locals {
  instance_common = {
    ami                    = data.aws_ami.ubuntu.id
    instance_type          = var.instance_type
    subnet_id              = aws_subnet.public.id
    vpc_security_group_ids = [aws_security_group.nodes.id]
    key_name               = aws_key_pair.admin.key_name
  }
}

resource "aws_instance" "server" {
  ami                    = local.instance_common.ami
  instance_type          = local.instance_common.instance_type
  subnet_id              = local.instance_common.subnet_id
  vpc_security_group_ids = local.instance_common.vpc_security_group_ids
  key_name               = local.instance_common.key_name

  user_data = templatefile("${path.module}/templates/k3s-server.sh.tftpl", {
    k3s_version = var.k3s_version
    token       = random_password.k3s_token.result
  })

  metadata_options {
    http_tokens = "required" # IMDSv2 only
  }
  root_block_device {
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }
  tags = { Name = "stormproof-k3s-server" }
}

resource "aws_instance" "agent" {
  count                  = 2
  ami                    = local.instance_common.ami
  instance_type          = local.instance_common.instance_type
  subnet_id              = local.instance_common.subnet_id
  vpc_security_group_ids = local.instance_common.vpc_security_group_ids
  key_name               = local.instance_common.key_name

  user_data = templatefile("${path.module}/templates/k3s-agent.sh.tftpl", {
    k3s_version = var.k3s_version
    token       = random_password.k3s_token.result
    server_ip   = aws_instance.server.private_ip
  })

  metadata_options {
    http_tokens = "required"
  }
  root_block_device {
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }
  tags = { Name = "stormproof-k3s-agent-${count.index + 1}" }
}

# A monthly budget alert, so a forgotten cluster can't quietly run up a bill.
resource "aws_budgets_budget" "monthly" {
  count        = var.budget_email == "" ? 0 : 1
  name         = "stormproof-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_email]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_email]
  }
}
