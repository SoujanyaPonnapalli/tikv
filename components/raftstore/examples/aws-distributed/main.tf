# Distributed TiKV bench: N tikv-server hosts + 1 controller (PD + client +
# orchestrator) in the same AZ + cluster placement group.
#
#   - All hosts: c6i.4xlarge (16 vCPU, 32 GiB, ~593 MB/s baseline EBS bw).
#   - Each TiKV host gets its own dedicated gp3 data volume (125 MB/s, 3000 IOPS).
#   - Controller has only the root volume (no provisioned-throughput EBS).
#   - Cluster placement group keeps inter-host RTT sub-ms.
#   - One security group allows SSH from ssh_cidr, all internal TCP between
#     instances (we don't bother restricting raft/PD ports).

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}
variable "key_name" {
  type = string
}
variable "ssh_cidr" {
  type = string
}
variable "num_tikv" {
  type    = number
  default = 3
}
variable "tikv_instance" {
  type    = string
  default = "c6i.4xlarge"
}
variable "ctl_instance" {
  type    = string
  default = "c6i.4xlarge"
}
variable "data_vol_gb" {
  type    = number
  default = 100
}

provider "aws" {
  region = var.region
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_placement_group" "cluster" {
  name     = "tikv-metronome-distributed"
  strategy = "cluster"
}

resource "aws_security_group" "sg" {
  name        = "tikv-metronome-distributed"
  vpc_id      = data.aws_vpc.default.id
  description = "SSH from operator; all TCP internal"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }
  ingress {
    description = "internal TCP for raft, gRPC, PD"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Pick a single subnet so all instances land in the same AZ — required for the
# cluster placement group.
locals { subnet_id = data.aws_subnets.default.ids[0] }

# Controller host: PD + go-ycsb + bench orchestrator. No dedicated data volume.
resource "aws_instance" "controller" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.ctl_instance
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.sg.id]
  subnet_id                   = local.subnet_id
  placement_group             = aws_placement_group.cluster.name
  associate_public_ip_address = true
  root_block_device {
    volume_type = "gp3"
    volume_size = 100
  }
  user_data = file("${path.module}/user_data_ctl.sh")
  tags = {
    Name = "tikv-metronome-dist-ctl"
  }
}

# TiKV hosts: one per replica, each with its own gp3 data volume.
resource "aws_instance" "tikv" {
  count                       = var.num_tikv
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.tikv_instance
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.sg.id]
  subnet_id                   = local.subnet_id
  placement_group             = aws_placement_group.cluster.name
  associate_public_ip_address = true
  root_block_device {
    volume_type = "gp3"
    volume_size = 50
  }
  user_data = file("${path.module}/user_data_tikv.sh")
  tags = {
    Name = "tikv-metronome-dist-tikv-${count.index + 1}"
  }
}

resource "aws_ebs_volume" "data" {
  count             = var.num_tikv
  availability_zone = aws_instance.tikv[count.index].availability_zone
  size              = var.data_vol_gb
  type              = "gp3"
  throughput        = 125
  iops              = 3000
  tags = {
    Name = "tikv-metronome-dist-data-${count.index + 1}"
  }
}

resource "aws_volume_attachment" "data" {
  count       = var.num_tikv
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.data[count.index].id
  instance_id = aws_instance.tikv[count.index].id
}

output "ctl_public_ip" {
  value = aws_instance.controller.public_ip
}
output "ctl_private_ip" {
  value = aws_instance.controller.private_ip
}
output "tikv_private_ips" {
  value = aws_instance.tikv[*].private_ip
}
output "tikv_public_ips" {
  value = aws_instance.tikv[*].public_ip
}
output "ssh_ctl_command" {
  value = "ssh -i <key.pem> ubuntu@${aws_instance.controller.public_ip}"
}
output "cost_note" {
  value = "Approx hourly: ctl ${var.ctl_instance} ~$0.68 + ${var.num_tikv} x ${var.tikv_instance} ~$0.68 ea + ${var.num_tikv} gp3 100GB ~$0.01 ea"
}
