// producer — отправляет сообщения в Kafka (Yandex Cloud Managed Kafka).
//
// Соединение: SASL/SCRAM-SHA-512 поверх TLS (порт 9091).
// Аутентификация: пользователь "producer", пароль из env KAFKA_PRODUCER_PASSWORD.
// TLS: CA-сертификат Яндекса из файла, путь в env KAFKA_CA_CERT (default: /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt).
// Брокеры: comma-separated FQDN в env KAFKA_BROKERS.
// Топик: env KAFKA_TOPIC (default: events).
package main

import (
	"crypto/tls"  // TLS-соединение — шифрование трафика между клиентом и брокером
	"crypto/x509" // Работа с X.509 сертификатами — добавляем CA Яндекса в пул доверенных
	"fmt"         // Форматированный вывод в консоль
	"log"         // Логирование ошибок
	"os"          // Переменные окружения (os.Getenv) и чтение файлов (os.ReadFile)
	"strings"     // Разбивка строки брокеров по запятой (strings.Split)
	"time"        // Время создания сообщения

	"github.com/IBM/sarama"                                    // Kafka-клиент
	kafkascram "github.com/an042/kafka-practical-work-7/scram" // Наш SCRAM-адаптер
)

func main() {
	// ─── Конфигурация из переменных окружения ─────────────────────────────────

	// Список брокеров — FQDN через запятую, например:
	// rc1a-xxx.mdb.yandexcloud.net:9091,rc1b-xxx.mdb.yandexcloud.net:9091
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

	// Пароль пользователя producer (задан при создании пользователя через Terraform)
	password := os.Getenv("KAFKA_PRODUCER_PASSWORD")
	if password == "" {
		log.Fatal("KAFKA_PRODUCER_PASSWORD не задан")
	}

	// Путь к CA-сертификату Яндекса (скачивается командой из outputs.tf)
	caFile := os.Getenv("KAFKA_CA_CERT")
	if caFile == "" {
		// Стандартный путь после `mkdir -p ... && wget -O ... CA.pem`
		caFile = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
	}

	// ─── TLS-конфигурация ─────────────────────────────────────────────────────

	// Читаем CA-сертификат Яндекса из файла
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Не удалось прочитать CA-сертификат %s: %v", caFile, err)
	}

	// Создаём пул доверенных CA и добавляем сертификат Яндекса
	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(caCert) // Парсим PEM-блок и добавляем в пул
	if !ok {
		log.Fatal("Не удалось добавить CA-сертификат в пул — проверь формат файла")
	}

	// TLS-конфиг: используем только CA Яндекса (mTLS не нужен, только server auth)
	tlsCfg := &tls.Config{
		RootCAs: caCertPool, // Только этому CA доверяем при проверке сертификата брокера
	}

	// ─── Sarama конфигурация ──────────────────────────────────────────────────

	cfg := sarama.NewConfig() // Создаём конфиг с дефолтными значениями

	// Версия протокола Kafka — Yandex Cloud Managed Kafka 3.x
	cfg.Version = sarama.V3_3_0_0

	// ─── SASL/SCRAM-SHA-512 ───────────────────────────────────────────────────
	cfg.Net.SASL.Enable = true                // Включаем SASL-аутентификацию
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512 // Механизм: SCRAM-SHA-512
	cfg.Net.SASL.User = "producer"            // Логин пользователя Kafka
	cfg.Net.SASL.Password = password          // Пароль из env
	cfg.Net.SASL.Handshake = true             // Использовать SASL handshake (обязательно для SCRAM)

	// SCRAMClientGeneratorFunc — фабрика SCRAM-клиентов.
	// sarama вызывает эту функцию для каждого нового соединения с брокером.
	// Возвращаем наш XDGSCRAMClient с алгоритмом SHA-512.
	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &kafkascram.XDGSCRAMClient{HashGeneratorFcn: kafkascram.SHA512}
	}

	// ─── TLS ──────────────────────────────────────────────────────────────────
	cfg.Net.TLS.Enable = true    // Включаем TLS-шифрование
	cfg.Net.TLS.Config = tlsCfg // Передаём наш TLS-конфиг с CA Яндекса

	// ─── Producer ─────────────────────────────────────────────────────────────
	// WaitForAll — ждать подтверждения от всех In-Sync реплик (ISR).
	// Наиболее безопасный режим: сообщение не потеряется даже при сбое лидера.
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	// Включаем идемпотентность — защита от дублирования при повторной отправке
	cfg.Producer.Idempotent = true
	// Для идемпотентного producer нужны точно 1 конкурентный запрос на брокер
	cfg.Net.MaxOpenRequests = 1
	// Возвращать успешно отправленные сообщения в канал Successes
	cfg.Producer.Return.Successes = true
	// Возвращать ошибки в канал Errors
	cfg.Producer.Return.Errors = true

	// ─── Создание SyncProducer ────────────────────────────────────────────────

	// SyncProducer блокирует горутину до получения ACK от брокера.
	// Проще в использовании, чем AsyncProducer — подходит для учебного примера.
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Не удалось создать producer: %v", err)
	}
	defer producer.Close() // Освобождаем ресурсы при завершении программы

	// ─── Отправка тестовых сообщений ─────────────────────────────────────────

	log.Printf("Producer подключён. Отправляю 10 сообщений в топик %q...", topic)

	for i := 1; i <= 10; i++ {
		// Формируем payload с порядковым номером и временной меткой
		value := fmt.Sprintf(`{"id":%d,"ts":"%s","source":"go-producer"}`,
			i, time.Now().Format(time.RFC3339))

		// ProducerMessage — структура одного сообщения Kafka
		msg := &sarama.ProducerMessage{
			Topic: topic,                                   // Куда отправляем
			Key:   sarama.StringEncoder(fmt.Sprintf("%d", i)), // Ключ = порядковый номер
			Value: sarama.StringEncoder(value),            // Тело сообщения в JSON
		}

		// SendMessage блокируется до получения ACK или ошибки
		// Возвращает: partition (в какую партицию попало), offset (позиция в партиции)
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Ошибка отправки сообщения %d: %v", i, err)
			continue // Продолжаем с следующим сообщением
		}

		log.Printf("Сообщение %d отправлено → partition=%d offset=%d", i, partition, offset)
	}

	log.Println("Готово!")
}
