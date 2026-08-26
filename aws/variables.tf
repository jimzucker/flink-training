variable "region" {
  description = "Everything lives in one region; MSK and the client must share a VPC."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  type    = string
  default = "flink-training"
}

# Started at c5.2xlarge to match the laptop's eight cores, which turned out to be
# a bad match rather than a fair one: those eight vCPUs are four physical Cascade
# Lake cores with hyperthreading, against eight real and faster cores on the
# laptop. Flink got roughly half the CPU, saturated at 92% while MSK sat at 0.66
# cores and 99.7% idle, and drained slower than the laptop had.
#
# So the client grows and the brokers stay: 32 vCPUs of Sapphire Rapids, four
# times the threads and quicker per thread. The point of the exercise is to find
# where Flink stops when the broker is not what stops it.
variable "client_instance_type" {
  type    = string
  default = "c7i.8xlarge"
}

# Three brokers, four vCPUs each. The local ceiling was one broker's write path
# at ~750,000 records/sec; this exists to not be that ceiling, so the question
# becomes what Flink does when the broker is not the thing stopping it.
variable "broker_instance_type" {
  type    = string
  default = "kafka.m5.xlarge"
}

# The 88M-order backlog is about 26GB of orders and roughly 90GB of positions.
variable "broker_volume_gb" {
  type    = number
  default = 250
}

variable "kafka_version" {
  description = "Matches the local stack's 3.9 KRaft line, so nothing differs but the hardware."
  type        = string
  default     = "3.9.x.kraft"
}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/terraform.pub"
}

variable "ssh_allowed_cidr" {
  description = "Only this address may reach the client instance."
  type        = string
}
