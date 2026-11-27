variable "region" {
  description = "AWS region. us-east-1 is usually the cheapest."
  type        = string
  default     = "us-east-1"
}

variable "admin_cidr" {
  description = "The only address allowed to SSH in, as a CIDR: your public IP with /32 (curl -s https://checkip.amazonaws.com)."
  type        = string
  validation {
    condition     = can(cidrhost(var.admin_cidr, 0)) && var.admin_cidr != "0.0.0.0/0"
    error_message = "admin_cidr must be a CIDR such as 203.0.113.7/32, and not 0.0.0.0/0."
  }
}

variable "ssh_public_key" {
  description = "Your SSH public key (the contents of ~/.ssh/id_ed25519.pub, for example)."
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type for all three nodes. t3.small (2 vCPU, 2 GB) fits k3s, a StormProof server and the monitoring."
  type        = string
  default     = "t3.small"
}

variable "k3s_version" {
  description = "k3s release to install (pinned, so every node runs the same one)."
  type        = string
  default     = "v1.34.13+k3s1"
}

variable "budget_email" {
  description = "If set, an AWS Budget emails this address when the month's actual or forecast spend passes budget_limit_usd. The first two budgets in an account are free."
  type        = string
  default     = ""
}

variable "budget_limit_usd" {
  description = "Monthly spend that triggers the budget alert."
  type        = number
  default     = 10
}
