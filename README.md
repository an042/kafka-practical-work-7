# Практическая работа 7 — Kafka в Yandex Cloud

Развёртывание Apache Kafka на базе Yandex Cloud Managed Service for Apache Kafka с интеграцией Apache NiFi.

## Стек

| Компонент | Версия |
|-----------|--------|
| Kafka (Managed) | 3.9 (Yandex Cloud) |
| Terraform | ≥ 1.0 + yandex-cloud/yandex 0.209.0 |
| Go client | 1.21, IBM/sarama v1.43.3 |
| SASL механизм | SCRAM-SHA-512 |
| Транспорт | SASL_SSL, порт 9091 |
| Schema Registry | Встроен в YC Managed Kafka, порт 443 (HTTPS) |
| Apache NiFi | 1.28.1 (Docker) |

## Структура

```
.
├── terraform/              # Инфраструктура (Yandex Cloud)
│   ├── main.tf             # Кластер, топик, пользователи
│   ├── variables.tf        # Входные переменные
│   ├── outputs.tf          # Выходные значения (FQDN брокеров, примеры подключения)
│   ├── terraform.tfvars    # Значения переменных (folder_id, subnet_ids и т.д.)
│   ├── versions.tf         # Версии provider и Terraform
│   └── modules/
│       └── kafka/cluster/  # Модуль кластера (из стартового zip)
├── client/                 # Go клиент (Задание 1)
│   ├── go.mod
│   ├── go.sum
│   ├── scram/
│   │   └── scram.go        # SCRAM-SHA-512 адаптер для sarama
│   └── cmd/
│       ├── producer/main.go
│       └── consumer/main.go
├── schema/
│   └── event.avsc          # Avro-схема сообщения (Задание 1)
├── nifi/                   # Apache NiFi (Задание 2)
│   └── docker-compose.yml  # Запуск NiFi 1.28.1 в Docker
└── docs/                   # Артефакты выполнения
    ├── producer_logs.txt         # Вывод Go producer
    ├── consumer_logs.txt         # Вывод Go consumer
    ├── schema_registry_output.txt # curl к Schema Registry
    ├── kafka_topic_describe.txt  # Описание топика и хостов кластера
    ├── hardware_resources.md     # Конфигурация узлов кластера
    └── nifi/
        ├── nifi_integration.md       # Настройка NiFi, проблемы и решения
        ├── nifi_output_messages.txt  # Сообщения, принятые NiFi из Kafka
        └── flow.json.gz              # Экспорт конфигурации NiFi Flow
```

---

## Задание 1. Развёртывание и настройка Kafka-кластера в Yandex Cloud

### Шаг 1. Развернуть кластер

Установи инструменты:

```bash
# Yandex Cloud CLI
curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash
yc init  # Авторизация через браузер

# Terraform (macOS)
brew install terraform
```

Авторизация Terraform:

```bash
export YC_TOKEN=$(yc iam create-token)  # токен живёт 1 час
```

Развёртывание:

```bash
cd terraform
terraform init          # Скачивает провайдер yandex-cloud/yandex
terraform plan          # Показывает что будет создано
terraform apply         # Создаёт кластер (~10-15 минут)
```

После завершения Terraform выведет FQDN брокеров. Или вручную:

```bash
yc managed-kafka cluster list-hosts --cluster-name kafka-practicum
# Брать только хосты с ROLE=KAFKA, не ZOOKEEPER
```

Установить CA-сертификат Яндекса:

```bash
mkdir -p ~/kafka-certs
curl -o ~/kafka-certs/YandexInternalRootCA.crt \
     https://storage.yandexcloud.net/cloud-certs/CA.pem
```

### Шаг 2. Настроить репликацию и хранение данных

Топик `events` создаётся через Terraform:
- 3 партиции, replication factor = 3
- `log.cleanup.policy = delete`
- `log.retention.ms = 604800000` (7 дней)
- `log.segment.bytes = 134217728` (128 МБ)

Параметры кластера и топика — в `docs/hardware_resources.md` и `docs/kafka_topic_describe.txt`.

### Шаг 3. Настроить Schema Registry

Schema Registry включён в YC Managed Kafka (`schema_registry = true` в Terraform).  
Endpoint: `https://<broker_fqdn>:443` — тот же хост, что и брокер, Basic Auth.

```bash
BROKER="rc1a-xxx.mdb.yandexcloud.net"

# Зарегистрировать схему
curl -u "producer:ProducerPass1" \
     -X POST \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --cacert ~/kafka-certs/YandexInternalRootCA.crt \
     "https://${BROKER}:443/subjects/events-value/versions" \
     -d "{\"schema\": $(cat schema/event.avsc | jq -Rs .)}"

# Просмотреть зарегистрированные схемы
curl -u "consumer:ConsumerPass1" \
     --cacert ~/kafka-certs/YandexInternalRootCA.crt \
     "https://${BROKER}:443/subjects"

curl -u "consumer:ConsumerPass1" \
     --cacert ~/kafka-certs/YandexInternalRootCA.crt \
     "https://${BROKER}:443/subjects/events-value/versions/1"
```

Вывод curl — в `docs/schema_registry_output.txt`.

### Шаг 4. Проверить работу Kafka (producer и consumer)

> **Важно про SCRAM в Go:**  
> IBM/sarama не включает реализацию SCRAM из коробки (в отличие от Python/Java).  
> Нужен внешний пакет `github.com/xdg-go/scram` и адаптер `client/scram/scram.go`.

```bash
brew install go  # macOS

cd client
go mod tidy      # Скачивает все зависимости, генерирует go.sum
```

Запустить producer:

```bash
export KAFKA_BROKERS="rc1a-xxx.mdb.yandexcloud.net:9091,rc1b-xxx.mdb.yandexcloud.net:9091,rc1d-xxx.mdb.yandexcloud.net:9091"
export KAFKA_PRODUCER_PASSWORD="ProducerPass1"
export KAFKA_CA_CERT="$HOME/kafka-certs/YandexInternalRootCA.crt"
export KAFKA_TOPIC="events"

cd client
go run ./cmd/producer/
```

Запустить consumer (в отдельном терминале):

```bash
export KAFKA_BROKERS="rc1a-xxx.mdb.yandexcloud.net:9091,..."
export KAFKA_CONSUMER_PASSWORD="ConsumerPass1"
export KAFKA_CA_CERT="$HOME/kafka-certs/YandexInternalRootCA.crt"
export KAFKA_TOPIC="events"
export KAFKA_GROUP="my-consumer-group"

go run ./cmd/consumer/
```

Логи producer и consumer — в `docs/producer_logs.txt` и `docs/consumer_logs.txt`.

---

## Задание 2. Интеграция Kafka с внешними системами (Apache NiFi)

Apache NiFi запускается локально в Docker и читает сообщения из YC Kafka по SASL_SSL.

### Флоу

```
ConsumeKafka_2_6 →[success]→ LogAttribute →[success]→ PutFile(/tmp/kafka-output)
```

### Запуск NiFi

```bash
# 1. Скачать CA-сертификат Яндекса
mkdir -p nifi/certs
curl -o nifi/certs/YandexInternalRootCA.crt \
  https://storage.yandexcloud.net/cloud-certs/CA.pem

# 2. Создать PKCS12 truststore через keytool
#    (важно: openssl не подходит — Java требует тип trustedCertEntry)
keytool -import \
  -alias yandex-root-ca \
  -file nifi/certs/YandexInternalRootCA.crt \
  -keystore nifi/certs/truststore.p12 \
  -storetype PKCS12 \
  -storepass truststorepass \
  -noprompt

# 3. Запустить контейнер
cd nifi && docker compose up -d
# UI: http://localhost:8080/nifi  (admin / admin12345678)
```

### Controller Service — StandardSSLContextService

Создаётся через **правый клик на холст → Configure → Controller Services** (не через меню ☰ — там Management-scope, процессоры его не видят).

| Параметр | Значение |
|---|---|
| Truststore Filename | `./conf/certs/truststore.p12` |
| Truststore Password | `truststorepass` |
| Truststore Type | `PKCS12` |
| TLS Protocol | `TLS` |

### Настройки ConsumeKafka_2_6

| Параметр | Значение |
|---|---|
| Kafka Brokers | `rc1a-xxx.mdb.yandexcloud.net:9091,...` |
| Topic Name(s) | `events` |
| Group ID | `nifi-consumer-group` |
| Security Protocol | `SASL_SSL` |
| SASL Mechanism | `SCRAM-SHA-512` |
| Username | `consumer` |
| Password | `ConsumerPass1` |
| SSL Context Service | `StandardSSLContextService` |

### Результат

NiFi принял 10 сообщений из топика `events` и записал в `/tmp/kafka-output/` — по одному файлу на сообщение. Подробности — в `docs/nifi/`.

---

## Удаление кластера

```bash
cd terraform
export YC_TOKEN=$(yc iam create-token)  # обнови токен, если прошло больше часа
terraform destroy   # Удаляет кластер, топик, пользователей (~5 минут)
```

**Внимание:** кластер тарифицируется пока запущен. Удаляй сразу после завершения работы.
