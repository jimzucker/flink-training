# Plaintext, unauthenticated, and reachable only from one security group inside
# one VPC. That matches the local broker exactly, which is the point: the jobs
# and the generator connect the same way they do on a laptop, so no code changes
# between the two runs and nothing but the hardware differs.
#
# It is not how a broker holding real trades would be configured. A durable
# deployment wants TLS in transit, IAM authentication and encryption at rest with
# a customer-managed key -- and this cluster is destroyed within the hour.
resource "aws_msk_cluster" "this" {
  cluster_name           = var.name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = length(local.msk_subnets)

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = local.msk_subnets
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_volume_gb
      }
    }
  }

  client_authentication {
    unauthenticated = true
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT"
      in_cluster    = false
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }
}

# The same broker settings the local stack was tuned to, so the comparison is not
# quietly measuring a configuration difference. Locally these bought a tenfold
# improvement in p95 produce latency and 2.9% -- nothing -- in throughput.
#
# queued.max.requests is absent, and not by choice: MSK rejects it as "not
# supported by at least one Apache Kafka version". The managed service exposes an
# allow-list rather than the whole of server.properties, which is worth knowing
# before planning a migration around a specific tuning. It is the depth of the
# queue between network threads and request handlers, so its absence means the
# handler count below is doing the work on its own.
resource "aws_msk_configuration" "this" {
  name           = "${var.name}-broker"
  kafka_versions = [var.kafka_version]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    num.io.threads=16
    num.network.threads=8
    socket.send.buffer.bytes=1048576
    socket.receive.buffer.bytes=1048576
    compression.type=producer
    default.replication.factor=1
    min.insync.replicas=1
    num.partitions=4
  PROPERTIES

  lifecycle {
    create_before_destroy = true
  }
}
