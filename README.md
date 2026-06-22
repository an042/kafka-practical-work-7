# Практическая работа 7 — Kafka в Yandex Cloud

Развёртывание Apache Kafka на базе Yandex Cloud Managed Service for Apache Kafka.

## Стек

| Компонент | Версия |
|-----------|--------|
| Kafka (Managed) | 3.x (Yandex Cloud) |
| Terraform | ≥ 1.0 |
| Yandex Cloud provider | ≥ 0.13 |
| Go client | 1.21 |
| SASL механизм | SCRAM-SHA-512 |
| Транспорт | SASL_SSL, порт 9091 |

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
│   ├── scram/
│   │   └── scram.go        # SCRAM-SHA-512 адаптер для sarama
│   └── cmd/
│       ├── producer/main.go
│       └── consumer/main.go
└── schema/
    └── event.avsc          # Avro-схема сообщения
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

## Удаление кластера

```bash
cd terraform
terraform destroy   # Удаляет кластер, топик, пользователей (~2 минуты)
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
