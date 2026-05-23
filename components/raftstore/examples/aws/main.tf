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
  type        = string
  default     = "us-east-1"
  description = "AWS region."
}

variable "key_name" {
  type        = string
  description = "Name of an existing EC2 key pair in the chosen region. Used for SSH access."
}

variable "ssh_cidr" {
  type        = string
  default     = "0.0.0.0/0"
  description = "CIDR allowed for SSH ingress. STRONGLY recommend setting this to your own a.b.c.d/32."
}

variable "instance_type" {
  type        = string
  default     = "c6i.8xlarge"
  description = "EC2 instance type. c6i.8xlarge gives 32 vCPU and ~1187 MiB/s EBS bandwidth, comfortably fits 7x125 MiB/s gp3 volumes."
}

variable "num_data_volumes" {
  type        = number
  default     = 7
  description = "Number of gp3 data volumes to attach (one per tikv-server in the N=7 case)."
}

variable "data_volume_size_gb" {
  type        = number
  default     = 100
}

variable "data_volume_throughput" {
  type        = number
  default     = 125
  description = "Per-volume gp3 throughput in MiB/s. 125 = gp3 default (no extra cost)."
}

variable "data_volume_iops" {
  type        = number
  default     = 3000
  description = "Per-volume gp3 IOPS. 3000 = gp3 default (no extra cost)."
}

provider "aws" {
  region = var.region
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

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

resource "aws_security_group" "tikv_bench" {
  name        = "tikv-metronome-bench"
  description = "SSH ingress + all egress for TiKV metronome benchmark host."
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "tikv_bench" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.tikv_bench.id]
  subnet_id                   = data.aws_subnets.default.ids[0]
  associate_public_ip_address = true

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
  }

  user_data = file("${path.module}/user_data.sh")

  tags = {
    Name    = "tikv-metronome-bench"
    Project = "tikv-metronome"
  }
}

resource "aws_ebs_volume" "data" {
  count             = var.num_data_volumes
  availability_zone = aws_instance.tikv_bench.availability_zone
  size              = var.data_volume_size_gb
  type              = "gp3"
  throughput        = var.data_volume_throughput
  iops              = var.data_volume_iops

  tags = {
    Name = "tikv-metronome-bench-data-${count.index + 1}"
  }
}

resource "aws_volume_attachment" "data" {
  count       = var.num_data_volumes
  device_name = "/dev/sd${element(["f", "g", "h", "i", "j", "k", "l"], count.index)}"
  volume_id   = aws_ebs_volume.data[count.index].id
  instance_id = aws_instance.tikv_bench.id
}

output "public_ip" {
  value = aws_instance.tikv_bench.public_ip
}

output "ssh_command" {
  value = "ssh -i <path/to/${var.key_name}.pem> ubuntu@${aws_instance.tikv_bench.public_ip}"
}

output "estimated_cost_note" {
  value = "c6i.8xlarge on-demand in us-east-1 is approximately $1.36/hr. 7 gp3 100 GiB volumes at 125 MiB/s default add approximately $0.11/hr. Total approximately $1.47/hr."
}
