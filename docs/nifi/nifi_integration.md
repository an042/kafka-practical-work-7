# Apache NiFi — интеграция с Yandex Cloud Managed Kafka

## Дата: 23.06.2026
## Кластер: c9qft9lu1andap9imai3 (kafka-practicum)

---

## Схема интеграции

```
Go Producer → Kafka (YC, SASL_SSL :9091) → NiFi (Docker, ConsumeKafka_2_6) → PutFile (/tmp/kafka-output)
```

---

## Стек

| Компонент | Версия / Где |
|---|---|
| Apache NiFi | 1.28.1 (Docker, localhost:8080) |
| Kafka | 3.9 (Yandex Cloud Managed, 3 брокера) |
| Протокол | SASL_SSL + SCRAM-SHA-512 |
| TLS | YandexInternalRootCA.crt |

---

## Запуск NiFi

```bash
# 1. Скачать CA-сертификат Яндекса
mkdir -p nifi/certs
curl -o nifi/certs/YandexInternalRootCA.crt \
  https://storage.yandexcloud.net/cloud-certs/CA.pem

# 2. Создать PKCS12 truststore через keytool (НЕ через openssl — Java требует trustedCertEntry)
keytool -import \
  -alias yandex-root-ca \
  -file nifi/certs/YandexInternalRootCA.crt \
  -keystore nifi/certs/truststore.p12 \
  -storetype PKCS12 \
  -storepass truststorepass \
  -noprompt

# 3. Запустить NiFi
cd nifi && docker compose up -d
# UI: http://localhost:8080/nifi (admin / admin12345678)
```

---

## Конфигурация Controller Service — StandardSSLContextService

| Параметр | Значение |
|---|---|
| Truststore Filename | `./conf/certs/truststore.p12` |
| Truststore Password | `truststorepass` |
| Truststore Type | `PKCS12` |
| TLS Protocol | `TLS` |

**Важно:** Controller Service создаётся через **правый клик на холст → Configure → Controller Services** (Process Group scope). Management Controller Services (из меню гамбургера) не доступен процессорам.

---

## Конфигурация процессора ConsumeKafka_2_6

| Параметр | Значение |
|---|---|
| Kafka Brokers | `rc1a-24sf7ejnsdaof02s.mdb.yandexcloud.net:9091, rc1b-57pji4mcisou8cnk.mdb.yandexcloud.net:9091, rc1d-1daqphb5mqn2a5q9.mdb.yandexcloud.net:9091` |
| Topic Name(s) | `events` |
| Group ID | `nifi-consumer-group` |
| Security Protocol | `SASL_SSL` |
| SASL Mechanism | `SCRAM-SHA-512` |
| Username | `consumer` |
| Password | `ConsumerPass1` |
| SSL Context Service | `StandardSSLContextService` (включён) |
| Offset Reset | `earliest` |

---

## Флоу в NiFi

```
ConsumeKafka_2_6 →[success]→ LogAttribute →[success]→ PutFile
```

| Процессор | Назначение |
|---|---|
| ConsumeKafka_2_6 | Читает сообщения из топика events |
| LogAttribute | Логирует метаданные каждого FlowFile (партиция, offset, ключ) |
| PutFile | Сохраняет содержимое сообщения в файл /tmp/kafka-output/ |

---

## Результат — полученные сообщения

NiFi принял 10 сообщений из топика `events`, отправленных Go producer'ом.
Каждое сообщение сохранено в отдельный файл в `/tmp/kafka-output/`:

```
{"id":1,"ts":"2026-06-23T16:31:02+03:00","source":"go-producer"}
{"id":2,"ts":"2026-06-23T16:31:02+03:00","source":"go-producer"}
{"id":3,"ts":"2026-06-23T16:31:02+03:00","source":"go-producer"}
{"id":4,"ts":"2026-06-23T16:31:03+03:00","source":"go-producer"}
{"id":5,"ts":"2026-06-23T16:31:03+03:00","source":"go-producer"}
{"id":6,"ts":"2026-06-23T16:31:03+03:00","source":"go-producer"}
{"id":7,"ts":"2026-06-23T16:31:03+03:00","source":"go-producer"}
{"id":8,"ts":"2026-06-23T16:31:03+03:00","source":"go-producer"}
{"id":9,"ts":"2026-06-23T16:31:03+03:00","source":"go-producer"}
{"id":10,"ts":"2026-06-23T16:31:03+03:00","source":"go-producer"}
```

---

## Ключевые проблемы и их решения

### 1. truststore.p12 — trustAnchors must be non-empty
**Проблема:** `openssl pkcs12 -export` создаёт certificate bags без признака trustedCertEntry — Java их игнорирует.

**Решение:** использовать `keytool -import` вместо openssl. Keytool проставляет атрибут trustedCertEntry, который Java PKCS12 provider распознаёт.

### 2. Permission denied на truststore.p12
**Проблема:** `docker cp` копирует файл с uid хост-пользователя (501), NiFi-процесс (uid nifi) не может читать.

**Решение:**
```bash
docker exec --user root nifi chmod 644 /opt/nifi/nifi-current/conf/certs/truststore.p12
docker exec --user root nifi chown nifi:nifi /opt/nifi/nifi-current/conf/certs/truststore.p12
```

### 3. Management vs Process Group Controller Services
**Проблема:** SSLContextService, созданный через меню ☰ → Controller Services (Management scope), недоступен процессорам.

**Решение:** правый клик на холст → Configure → Controller Services → добавить здесь (Process Group scope).
