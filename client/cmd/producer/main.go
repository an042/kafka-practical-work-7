// producer — отправляет сообщения в Kafka (Yandex Cloud Managed Kafka).
//
// Соединение: SASL/SCRAM-SHA-512 поверх TLS (порт 9091).
// Сериализация: Avro (goavro/v2) в формате Confluent Wire Format:
//   байт 0x00 (magic) + 4 байта schema_id (big-endian) + avro-бинарник.
// Schema ID передаётся через env KAFKA_SCHEMA_ID (default: 1).
package main

import (
	"bytes"           // Буфер для сборки Confluent Wire Format сообщения
	"crypto/tls"      // TLS-соединение — шифрование трафика между клиентом и брокером
	"crypto/x509"     // Работа с X.509 сертификатами — добавляем CA Яндекса в пул доверенных
	"encoding/binary" // Запись schema_id как 4-байтового big-endian int32
	"fmt"             // Форматированный вывод
	"log"             // Логирование ошибок
	"os"              // Переменные окружения и чтение файлов
	"strconv"         // Парсинг schema_id из строки
	"strings"         // Разбивка строки брокеров по запятой
	"time"            // Время создания события

	"github.com/IBM/sarama"                                    // Kafka-клиент
	goavro "github.com/linkedin/goavro/v2"                    // Avro-кодек
	kafkascram "github.com/an042/kafka-practical-work-7/scram" // Наш SCRAM-адаптер
)

// avroSchema — Avro-схема события, совпадает со schema/event.avsc.
// Встроена в код, чтобы producer не зависел от пути к файлу схемы.
const avroSchema = `{
  "type": "record",
  "name": "Event",
  "namespace": "ru.practicum.kafka",
  "fields": [
    {"name": "id",      "type": "int"},
    {"name": "ts",      "type": "string"},
    {"name": "source",  "type": "string"},
    {"name": "payload", "type": ["null", "string"], "default": null}
  ]
}`

func main() {
	// ─── Конфигурация из переменных окружения ─────────────────────────────────

	// Список брокеров — FQDN через запятую
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		log.Fatal("KAFKA_BROKERS не задан — укажи FQDN брокеров через запятую")
	}
	brokers := strings.Split(brokersEnv, ",") // Разбиваем строку в срез

	// Имя топика для отправки сообщений
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "events" // Дефолтное имя, совпадает с terraform.tfvars
	}

	// Пароль пользователя producer
	password := os.Getenv("KAFKA_PRODUCER_PASSWORD")
	if password == "" {
		log.Fatal("KAFKA_PRODUCER_PASSWORD не задан")
	}

	// Путь к CA-сертификату Яндекса
	caFile := os.Getenv("KAFKA_CA_CERT")
	if caFile == "" {
		caFile = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
	}

	// ID схемы в Schema Registry (получен при регистрации через curl).
	// При первой регистрации Schema Registry возвращает {"id":1}.
	schemaIDStr := os.Getenv("KAFKA_SCHEMA_ID")
	if schemaIDStr == "" {
		schemaIDStr = "1" // Дефолт: первая зарегистрированная схема
	}
	schemaIDParsed, err := strconv.ParseInt(schemaIDStr, 10, 32)
	if err != nil {
		log.Fatalf("KAFKA_SCHEMA_ID должен быть числом: %v", err)
	}
	schemaID := int32(schemaIDParsed) // ID схемы как 4-байтовый int32

	// ─── Avro-кодек ───────────────────────────────────────────────────────────

	// NewCodec парсит JSON-схему и создаёт кодек для кодирования/декодирования
	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		log.Fatalf("Не удалось создать Avro-кодек: %v", err)
	}

	// ─── TLS-конфигурация ─────────────────────────────────────────────────────

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Не удалось прочитать CA-сертификат %s: %v", caFile, err)
	}

	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(caCert) // Парсим PEM и добавляем в пул доверенных
	if !ok {
		log.Fatal("Не удалось добавить CA-сертификат в пул — проверь формат файла")
	}

	tlsCfg := &tls.Config{
		RootCAs: caCertPool, // Только CA Яндекса при проверке сертификата брокера
	}

	// ─── Sarama конфигурация ──────────────────────────────────────────────────

	cfg := sarama.NewConfig()

	cfg.Version = sarama.V3_3_0_0 // Версия протокола Kafka

	// SASL/SCRAM-SHA-512
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	cfg.Net.SASL.User = "producer"
	cfg.Net.SASL.Password = password
	cfg.Net.SASL.Handshake = true
	// Фабрика SCRAM-клиентов — sarama вызывает для каждого нового соединения
	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &kafkascram.XDGSCRAMClient{HashGeneratorFcn: kafkascram.SHA512}
	}

	// TLS
	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = tlsCfg

	// Producer: ждать подтверждения от всех ISR-реплик (наиболее надёжный режим)
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	// Идемпотентность: защита от дублирования при повторной отправке
	cfg.Producer.Idempotent = true
	// Для идемпотентного producer — ровно 1 конкурентный запрос на брокер
	cfg.Net.MaxOpenRequests = 1
	// Возвращать результаты (partition, offset) и ошибки через каналы
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	// ─── Создание SyncProducer ────────────────────────────────────────────────

	// SyncProducer блокирует до получения ACK — удобен для учебного примера
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Не удалось создать producer: %v", err)
	}
	defer producer.Close()

	// ─── Отправка сообщений в Avro-формате ────────────────────────────────────

	log.Printf("Producer подключён. Отправляю 10 сообщений в топик %q (Avro, schema_id=%d)...", topic, schemaID)

	for i := 1; i <= 10; i++ {
		// Собираем нативную Go-структуру, соответствующую Avro-схеме Event.
		// Поле payload — union ["null","string"]: для значения null передаём
		// map{"null": nil}, для строки — map{"string": "значение"}.
		native := map[string]interface{}{
			"id":      i,                            // int
			"ts":      time.Now().Format(time.RFC3339), // string
			"source":  "go-producer",               // string
			"payload": map[string]interface{}{"null": nil}, // union → null
		}

		// BinaryFromNative кодирует native-значение в бинарный Avro-формат.
		// nil в первом аргументе означает "создать новый буфер".
		avroBinary, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			log.Printf("Ошибка Avro-кодирования сообщения %d: %v", i, err)
			continue
		}

		// ── Confluent Wire Format ──────────────────────────────────────────────
		// Стандарт Confluent для сообщений со Schema Registry:
		//   [0x00][schema_id 4 bytes big-endian][avro binary payload]
		// Consumer и Schema Registry ориентируются по magic byte (0x00)
		// и schema_id, чтобы выбрать правильный кодек для декодирования.
		var buf bytes.Buffer
		buf.WriteByte(0x00)                                       // Magic byte — признак Confluent Wire Format
		_ = binary.Write(&buf, binary.BigEndian, schemaID)       // Schema ID: 4 байта, big-endian
		buf.Write(avroBinary)                                     // Avro-закодированное тело сообщения

		// ProducerMessage — одно сообщение Kafka
		msg := &sarama.ProducerMessage{
			Topic: topic,                                        // Целевой топик
			Key:   sarama.StringEncoder(fmt.Sprintf("%d", i)), // Ключ = порядковый номер
			Value: sarama.ByteEncoder(buf.Bytes()),             // Тело = Confluent Wire Format + Avro
		}

		// Отправляем — блокируемся до ACK
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Ошибка отправки сообщения %d: %v", i, err)
			continue
		}

		log.Printf("Сообщение %d отправлено → partition=%d offset=%d (avro, %d байт)",
			i, partition, offset, buf.Len())
	}

	log.Println("Готово!")
}
