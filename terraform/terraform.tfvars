# Имя кластера (отображается в консоли YC)
name = "kafka-practicum"

# Каталог default в cloud-anetishin
folder_id = "b1gkkc5c6r2btrs6ch95"

# Сеть default
network_id = "enp7h15721pj53b44vn0"

# Количество брокеров — по одному на зону
brokers_count = 3

# Подсети: a, b, d (порядок совпадает с zones!)
subnet_ids = [
  "e9bl98rnaf8jq075dsfq",  # default-ru-central1-a
  "e2lhepijnstd23qulrqj",  # default-ru-central1-b
  "fl8jg5oa6a9lg49htbd3"   # default-ru-central1-d
]

# Зоны доступности (порядок совпадает с subnet_ids!)
zones = [
  "ru-central1-a",
  "ru-central1-b",
  "ru-central1-d"
]

# Включить Schema Registry (нужен для задания 1)
schema_registry = true

# Ресурсы брокера — s3-c2-m8 = 2 vCPU, 8 GB RAM (минимум для Kafka в YC)
resource_preset_id = "s3-c2-m8"

# Тип и размер диска
disk_type_id = "network-ssd"
disk_size    = 10

# Имя топика
topic_name = "events"

# Пароли пользователей (SASL/SCRAM-SHA-512)
# Требования YC: минимум 8 символов, заглавные + строчные + цифры
producer_password = "ProducerPass1"
consumer_password = "ConsumerPass1"
