output "bootstrap_brokers" {
  description = "Plaintext bootstrap string, for KAFKA_INTERNAL and BOOTSTRAP."
  value       = aws_msk_cluster.this.bootstrap_brokers
}

output "client_public_ip" {
  value = aws_instance.client.public_ip
}

output "ssh" {
  value = "ssh -i ~/.ssh/terraform ec2-user@${aws_eip.client.public_ip}"
}

output "broker_count" {
  value = aws_msk_cluster.this.number_of_broker_nodes
}
