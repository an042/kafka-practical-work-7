# Получаем данные текущего авторизованного клиента YC (folder_id из окружения)
data "yandex_client_config" "client" {}

# Определяем folder_id: либо из переменной, либо из yc CLI
locals {
  folder_id = var.folder_id == null ? data.yandex_client_config.client.folder_id : var.folder_id
}

# ─── Модуль: Kafka-кластер ────────────────────────────────────────────────────
# Создаёт Managed Service for Apache Kafka с 3 брокерами и ZooKeeper
module "kafka_cluster" {
  source = "./modules/kafka/cluster"

  name              = var.name           # Имя кластера в YC
  network_id        = var.network_id     # ID сети VPC
  subnet_ids        = var.subnet_ids     # Подсети для брокеров (по одной на зону)
  zones             = var.zones          # Зоны доступности брокеров
  brokers_count     = var.brokers_count  # 3 брокера = отказоустойчивость при потере 1
  folder_id         = local.folder_id    # Каталог для размещения кластера
  schema_registry   = var.schema_registry  # Включаем Schema Registry (нужен для задания 1)
  assign_public_ip  = true               # Публичный IP — доступ с локальной машины

  # Параметры дисков брокеров
  disk_type_id       = var.disk_type_id      # network-ssd для лучшей производительности
  disk_size          = var.disk_size         # 10 ГБ — достаточно для учебного кластера
  resource_preset_id = var.resource_preset_id # Тип инстанса

  # Настройки Kafka — соответствуют требованиям задания 1
  kafka_config = {
    # Политика очистки: delete = удалять сообщения после истечения retention
    # (альтернатива: compact = хранить только последнее сообщение для каждого ключа)
    log_retention_ms    = 604800000    # Хранить логи 7 дней (604800000 мс = 7 * 24 * 60 * 60 * 1000)
    log_segment_bytes   = 134217728    # Размер одного сегмента лога: 128 МБ (128 * 1024 * 1024)
    num_partitions      = 3            # Число партиций по умолчанию для новых топиков
    default_replication_factor = 3     # Коэффициент репликации по умолчанию (копия на каждом брокере)
    # SCRAM-SHA-512 — единственный поддерживаемый механизм SASL в Yandex Cloud Kafka
    sasl_enabled_mechanisms = ["SCRAM_SHA_512"]
  }

  # ZooKeeper-ноды (автоматически создаются при brokers_count > 1)
  # Используем минимальный размер для экономии гранта
  zookeeper_config = {
    resources = {
      resource_preset_id = "s3-c2-m8"  # Тип инстанса ZooKeeper
      disk_type_id       = "network-ssd"
      disk_size          = 10           # 10 ГБ — достаточно для ZK
    }
  }
}

# ─── Топик: events ───────────────────────────────────────────────────────────
# Основной топик для тестовых сообщений (задание 1)
resource "yandex_mdb_kafka_topic" "events" {
  cluster_id         = module.kafka_cluster.cluster_id  # Привязываем к нашему кластеру
  name               = var.topic_name   # Имя топика из переменной (default: "events")
  partitions         = 3                # 3 партиции = параллельная обработка на 3 брокерах
  replication_factor = 3                # RF=3 = каждая партиция на всех 3 брокерах

  topic_config {
    # Политика очистки: DELETE = удалять старые сообщения по времени или размеру
    cleanup_policy  = "CLEANUP_POLICY_DELETE"
    # Хранить сообщения не дольше 7 дней
    retention_ms    = 604800000
    # Максимальный размер одного сегмента файла лога: 128 МБ
    segment_bytes   = 134217728
  }
}

# ─── Пользователь: producer ──────────────────────────────────────────────────
# Имеет права только на запись в топик events
resource "yandex_mdb_kafka_user" "producer" {
  cluster_id = module.kafka_cluster.cluster_id
  name       = "producer"              # Логин для SASL-аутентификации
  password   = var.producer_password   # Пароль из terraform.tfvars (sensitive)

  permission {
    topic_name = var.topic_name           # Доступ к топику events
    role       = "ACCESS_ROLE_PRODUCER"   # Только запись (write)
  }
}

# ─── Пользователь: consumer ──────────────────────────────────────────────────
# Имеет права только на чтение из топика events
resource "yandex_mdb_kafka_user" "consumer" {
  cluster_id = module.kafka_cluster.cluster_id
  name       = "consumer"
  password   = var.consumer_password

  permission {
    topic_name = var.topic_name           # Доступ к топику events
    role       = "ACCESS_ROLE_CONSUMER"   # Только чтение (read)
  }
}
