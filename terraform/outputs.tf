# ID кластера — нужен для управления через CLI
output "cluster_id" {
  description = "ID Kafka-кластера в Yandex Cloud"
  value       = module.kafka_cluster.cluster_id
}

# Список FQDN брокеров — адреса для подключения клиентов
# Формат: rc1a-xxx.mdb.yandexcloud.net:9091 (порт 9091 = SSL)
output "brokers" {
  description = "FQDN хостов кластера (порт подключения: 9091)"
  value       = module.kafka_cluster.cluster_host_names_list
}

# Шаг 1: Скачать CA-сертификат Яндекса (нужен для TLS клиентов)
output "install_ca_cert" {
  description = "Команда для установки CA-сертификата Яндекса"
  value       = module.kafka_cluster.connection_step_1
}

# Шаг 2: Пример подключения через kafkacat
output "connection_example" {
  description = "Пример подключения через kafkacat"
  value       = module.kafka_cluster.connection_step_2
}

# Endpoint Schema Registry — HTTPS, тот же хост что и брокеры, порт 443
output "schema_registry_url" {
  description = "URL Schema Registry (HTTPS, порт 443)"
  value       = "https://<broker_fqdn>:443 — см. brokers output, замени <broker_fqdn>"
}
