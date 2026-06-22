# Имя кластера Kafka — будет отображаться в консоли YC
variable "name" {
  description = "Имя Kafka-кластера"
  type        = string
}

# ID облачной сети (VPC) из Yandex Cloud
variable "network_id" {
  description = "ID сети VPC в Yandex Cloud"
  type        = string
}

# ID подсетей — по одной на каждую зону доступности брокера
variable "subnet_ids" {
  description = "Список ID подсетей (по одной на зону)"
  type        = list(string)
}

# Зоны доступности — должны соответствовать порядку subnet_ids
variable "zones" {
  description = "Список зон доступности (ru-central1-a, -b, -d)"
  type        = list(string)
}

# Количество брокеров Kafka в кластере
variable "brokers_count" {
  description = "Количество брокеров (рекомендуется 3 для отказоустойчивости)"
  type        = number
}

# ID каталога YC (если null — берётся из yc CLI)
variable "folder_id" {
  description = "ID каталога Yandex Cloud (null = из окружения)"
  type        = string
  default     = null
}

# Включить Schema Registry (нужен для задания 1 — регистрации схем Avro/JSON)
variable "schema_registry" {
  description = "Включить встроенный Schema Registry"
  type        = bool
  default     = true
}

# Тип диска: network-ssd — быстрее, network-hdd — дешевле
variable "disk_type_id" {
  description = "Тип диска брокера"
  type        = string
  default     = "network-ssd"
}

# Размер диска в ГБ — достаточно 10 для учебного задания
variable "disk_size" {
  description = "Размер диска брокера в ГБ"
  type        = number
  default     = 10
}

# Тип инстанса брокера. s3-c2-m8 = 2 vCPU, 8 GB RAM
# Минимальный для Kafka в YC: s3-c2-m8
variable "resource_preset_id" {
  description = "Тип инстанса брокера"
  type        = string
  default     = "s3-c2-m8"
}

# Имя топика для тестирования
variable "topic_name" {
  description = "Имя основного топика"
  type        = string
  default     = "events"
}

# Пароль producer-пользователя (изменить перед деплоем!)
variable "producer_password" {
  description = "Пароль пользователя producer"
  type        = string
  sensitive   = true  # не выводится в логах Terraform
}

# Пароль consumer-пользователя
variable "consumer_password" {
  description = "Пароль пользователя consumer"
  type        = string
  sensitive   = true
}
