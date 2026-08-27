# Runs what the laptop ran: the Flink job manager and task manager from the same
# compose file, the same generator jar, and Prometheus so the measurement scripts
# work unchanged. Only the bootstrap servers differ.
resource "aws_instance" "client" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.client_instance_type
  subnet_id                   = local.client_subnet
  vpc_security_group_ids      = [aws_security_group.client.id]
  key_name                    = aws_key_pair.client.key_name
  associate_public_ip_address = true

  root_block_device {
    volume_size = 100
    volume_type = "gp3"
  }

  user_data = <<-SCRIPT
    #!/bin/bash
    set -euxo pipefail
    dnf install -y docker git java-17-amazon-corretto-headless
    systemctl enable --now docker
    usermod -aG docker ec2-user
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-compose \
      https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
    dnf install -y maven || true
    sudo -u ec2-user git clone --depth 1 https://github.com/jimzucker/flink-training.git /home/ec2-user/flink-training
    touch /home/ec2-user/.provisioned
  SCRIPT

  tags = { Name = "${var.name}-client" }
}

# A stable address. Without one, every instance resize hands back a new public IP
# -- the address is released on stop and a different one assigned on start -- so
# any open SSH tunnel dies and every command carrying the old address fails
# quietly. That cost a working tunnel the first time the client was resized.
#
# Charged either way: AWS bills every public IPv4 address, attached or not, at
# about half a cent an hour. This makes the address survive a stop rather than
# adding a cost that was not already there.
resource "aws_eip" "client" {
  instance = aws_instance.client.id
  domain   = "vpc"
  tags     = { Name = "${var.name}-client" }
}
