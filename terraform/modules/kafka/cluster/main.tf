
# Cluster
resource "yandex_mdb_kafka_cluster" "this" {
  name                = var.name
  description         = var.description
  environment         = var.environment
  network_id          = var.network_id
  subnet_ids          = var.subnet_ids
  folder_id           = var.folder_id
  security_group_ids  = var.security_groups_ids_list
  deletion_protection = var.deletion_protection
  labels              = var.labels

  config {
    version          = var.kafka_version
    brokers_count    = var.brokers_count
    zones            = var.zones
    assign_public_ip = var.assign_public_ip
    schema_registry  = var.schema_registry

    kafka {
      resources {
        resource_preset_id = var.resource_preset_id
        disk_type_id       = var.disk_type_id
        disk_size          = var.disk_size
      }
      kafka_config {
        compression_type                = var.kafka_config.compression_type
        log_flush_interval_messages     = var.kafka_config.log_flush_interval_messages
        log_flush_interval_ms           = var.kafka_config.log_flush_interval_ms
        log_flush_scheduler_interval_ms = var.kafka_config.log_flush_scheduler_interval_ms
        log_retention_bytes             = var.kafka_config.log_retention_bytes
        log_retention_hours             = var.kafka_config.log_retention_hours
        log_retention_minutes           = var.kafka_config.log_retention_minutes
        log_retention_ms                = var.kafka_config.log_retention_ms
        log_segment_bytes               = var.kafka_config.log_segment_bytes
        log_preallocate                 = var.kafka_config.log_preallocate
        num_partitions                  = var.kafka_config.num_partitions
        default_replication_factor      = var.kafka_config.default_replication_factor
        message_max_bytes               = var.kafka_config.message_max_bytes
        replica_fetch_max_bytes         = var.kafka_config.replica_fetch_max_bytes
        ssl_cipher_suites               = var.kafka_config.ssl_cipher_suites
        offsets_retention_minutes       = var.kafka_config.offsets_retention_minutes
        sasl_enabled_mechanisms         = var.kafka_config.sasl_enabled_mechanisms
      }
    }
    dynamic "zookeeper" {
      for_each = var.brokers_count > 1 ? [1] : []
      content {
        resources {
          resource_preset_id = var.zookeeper_config.resources.resource_preset_id
          disk_type_id       = var.zookeeper_config.resources.disk_type_id
          disk_size          = var.zookeeper_config.resources.disk_size
        }
      }
    }
    dynamic "access" {
      for_each = range(var.access_policy == null ? 0 : 1)
      content {
        data_transfer = var.access_policy.data_transfer
      }
    }
  }

  dynamic "maintenance_window" {
    for_each = range(var.maintenance_window == null ? 0 : 1)
    content {
      type = var.maintenance_window.type
      day  = var.maintenance_window.day
      hour = var.maintenance_window.hour
    }
  }
}
