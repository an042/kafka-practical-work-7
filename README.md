# Практическая работа 7 — Kafka в Yandex Cloud

Развёртывание Apache Kafka на базе Yandex Cloud Managed Service for Apache Kafka с интеграцией Apache NiFi.

## Стек

| Компонент | Версия |
|-----------|--------|
| Kafka (Managed) | 3.9 (Yandex Cloud) |
| Terraform | ≥ 1.0 |
| Yandex Cloud provider | 0.209.0 |
| Go client | 1.21 |
| SASL механизм | SCRAM-SHA-512 |
| Транспорт | SASL_SSL, порт 9091 |
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
├── client/                 # Go клиент
│   ├── go.mod
│   ├── go.sum
│   ├── scram/
│   │   └── scram.go        # SCRAM-SHA-512 адаптер для sarama
│   └── cmd/
│       ├── producer/main.go
│       └── consumer/main.go
├── schema/
│   └── event.avsc          # Avro-схема сообщения
├── nifi/                   # Apache NiFi (Задание 4)
│   └── docker-compose.yml  # Запуск NiFi 1.28.1 в Docker
└── docs/                   # Артефакты выполнения
    ├── producer_logs.txt         # Вывод Go producer
    ├── consumer_logs.txt         # Вывод Go consumer
    ├── schema_registry_output.txt # curl к Schema Registry
    ├── kafka_topic_describe.txt  # Описание топика и хостов кластера
    ├── hardware_resources.md     # Конфигурация узлов кластера
    └── nifi/
        ├── nifi_integration.md       # Описание настройки NiFi, проблемы и решения
        ├── nifi_output_messages.txt  # Сообщения, принятые NiFi из Kafka
        └── flow.json.gz              # Экспорт конфигурации NiFi Flow
```

---

## Задание 1 — Развернуть кластер Kafka

### 1.1 Подготовка

Установи инструменты:

```bash
# Yandex Cloud CLI
curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash
yc init  # Авторизация через браузер

# Terraform (macOS)
brew install terraform
```

### 1.2 Авторизация Terraform в YC

```bash
# Экспортируй IAM-токен как переменную окружения
export YC_TOKEN=$(yc iam create-token)

# Проверь что folder_id и network_id правильные:
yc resource-manager folder list
yc vpc network list
yc vpc subnet list
```

### 1.3 Развёртывание

```bash
cd terraform

terraform init          # Скачивает провайдер yandex-cloud/yandex
terraform plan          # Показывает что будет создано (без изменений)
terraform apply         # Создаёт кластер (~10-15 минут)
```

Кластер создаётся долго — это нормально. После завершения Terraform выведет:

```
cluster_id        = "mdb..."
brokers           = ["rc1a-xxx.mdb.yandexcloud.net", ...]
install_ca_cert   = "mkdir -p ... && wget -O ..."
connection_example = "kcat -b rc1a-xxx:9091 ..."
schema_registry_url = "https://rc1a-xxx.mdb.yandexcloud.net:443"
```

### 1.4 Получить FQDN брокеров вручную

Если нужно — через CLI:

```bash
yc managed-kafka cluster list-hosts --cluster-name kafka-practicum
```

### 1.5 Установить CA-сертификат Яндекса

Скопируй команду из output `install_ca_cert` и выполни:

```bash
mkdir -p /usr/local/share/ca-certificates/Yandex
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
     -O /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt
```

---

## Задание 2 — Schema Registry

Yandex Cloud включает Schema Registry при `schema_registry = true` в Terraform.  
Endpoint доступен по HTTPS на том же хосте, что и брокер, порт 443.

### Зарегистрировать схему

```bash
# BROKER — один из FQDN из output brokers, без порта
BROKER="rc1a-xxx.mdb.yandexcloud.net"
SCHEMA_URL="https://${BROKER}:443"

# Регистрируем схему события
curl -u "producer:ProducerPass1" \
     -X POST \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --cacert /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
     "${SCHEMA_URL}/subjects/events-value/versions" \
     -d "{\"schema\": $(cat ../schema/event.avsc | jq -Rs .)}"
```

### Просмотреть зарегистрированные схемы

```bash
curl -u "consumer:ConsumerPass1" \
     --cacert /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
     "${SCHEMA_URL}/subjects"

curl -u "consumer:ConsumerPass1" \
     --cacert /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
     "${SCHEMA_URL}/subjects/events-value/versions/1"
```

---

## Задание 3 — Go клиент (SCRAM-SHA-512)

> **Важно про SCRAM в Go:**  
> IBM/sarama не включает реализацию SCRAM из коробки (в отличие от Python/Java).  
> Нужен внешний пакет `github.com/xdg-go/scram` и адаптер `client/scram/scram.go`.

### Установить Go и зависимости

```bash
brew install go  # macOS

cd client
go mod tidy      # Скачивает все зависимости, генерирует go.sum
```

### Запустить producer

```bash
export KAFKA_BROKERS="rc1a-xxx.mdb.yandexcloud.net:9091,rc1b-xxx.mdb.yandexcloud.net:9091,rc1d-xxx.mdb.yandexcloud.net:9091"
export KAFKA_PRODUCER_PASSWORD="ProducerPass1"
export KAFKA_CA_CERT="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
export KAFKA_TOPIC="events"

cd client
go run ./cmd/producer/
```

Ожидаемый вывод:

```
Producer подключён. Отправляю 10 сообщений в топик "events"...
Сообщение 1 отправлено → partition=0 offset=0
Сообщение 2 отправлено → partition=1 offset=0
...
```

### Запустить consumer

```bash
export KAFKA_BROKERS="rc1a-xxx.mdb.yandexcloud.net:9091,..."
export KAFKA_CONSUMER_PASSWORD="ConsumerPass1"
export KAFKA_CA_CERT="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
export KAFKA_TOPIC="events"
export KAFKA_GROUP="my-consumer-group"

go run ./cmd/consumer/
```

Ожидаемый вывод:

```
Consumer запущён. Группа: "my-consumer-group", топик: "events". Ctrl+C для выхода.
Начинаю читать: топик=events, партиция=0, начальный offset=0
Получено: partition=0 offset=0 key=1 value={"id":1,"ts":"2026-06-22T...","source":"go-producer"}
...
```

---

---

## Задание 4 — Apache NiFi (интеграция с Kafka)

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

Создаётся через **правый клик на холст → Configure → Controller Services** (не через меню ☰, там Management-scope, процессоры его не видят).

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

После запуска Go producer NiFi принял 10 сообщений и записал в `/tmp/kafka-output/` — по одному файлу на сообщение. Подробности и вывод — в `docs/nifi/`.

---

## Удаление кластера

```bash
cd terraform
export YC_TOKEN=$(yc iam create-token)  # токен живёт 1 час, обнови если устарел
terraform destroy   # Удаляет кластер, топик, пользователей (~5 минут)
```

**Внимание:** кластер тарифицируется пока запущен. Удаляй после завершения работы.

---

## Подключение через kcat (проверка)

```bash
# Установить
brew install kcat

# Отправить сообщение
echo '{"test":1}' | kcat \
  -b rc1a-xxx.mdb.yandexcloud.net:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=producer \
  -X sasl.password=ProducerPass1 \
  -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
  -P -t events

# Прочитать все сообщения
kcat \
  -b rc1a-xxx.mdb.yandexcloud.net:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=consumer \
  -X sasl.password=ConsumerPass1 \
  -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
  -C -t events -o beginning -e
```
