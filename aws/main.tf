# The default VPC rather than a purpose-built one. This stack exists to answer a
# throughput question for an hour and then be destroyed; a bespoke VPC would add
# a NAT gateway, its hourly charge and its data processing fee, and would change
# nothing about the measurement.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_subnet" "by_id" {
  for_each = toset(data.aws_subnets.default.ids)
  id       = each.value
}

# MSK wants one subnet per broker and they must be in different zones. Sorted so
# a re-plan does not reshuffle which three are used.
locals {
  subnets_by_az = { for s in data.aws_subnet.by_id : s.availability_zone => s.id }
  # Two zones rather than three. MSK requires at least two, and every extra zone
  # is another slice of Kafka traffic crossing a zone boundary at a cent per
  # gigabyte each way -- which on the step 11 run cost more than the servers did.
  msk_azs       = slice(sort(keys(local.subnets_by_az)), 0, var.broker_count)
  msk_subnets   = [for az in local.msk_azs : local.subnets_by_az[az]]
  client_subnet = local.msk_subnets[0]
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-kernel-6.*-x86_64"]
  }
}

resource "aws_key_pair" "client" {
  key_name   = "${var.name}-client"
  public_key = file(pathexpand(var.ssh_public_key_path))
}

resource "aws_security_group" "client" {
  name        = "${var.name}-client"
  description = "Flink, the generator and Prometheus"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH from the operator only"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "msk" {
  name        = "${var.name}-msk"
  description = "Brokers, reachable only from the client instance"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "Kafka plaintext from the client"
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
    security_groups = [aws_security_group.client.id]
  }

  # MSK's open monitoring: the JMX exporter on 11001 and the node exporter on
  # 11002, one pair per broker. Enabling open_monitoring on the cluster publishes
  # these, but nothing can reach them until the port is open -- which is easy to
  # miss, and leaves a dashboard of empty broker panels as the only symptom.
  ingress {
    description     = "Prometheus scraping the brokers"
    from_port       = 11001
    to_port         = 11002
    protocol        = "tcp"
    security_groups = [aws_security_group.client.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
