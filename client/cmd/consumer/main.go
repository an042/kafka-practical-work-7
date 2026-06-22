// consumer — читает сообщения из Kafka (Yandex Cloud Managed Kafka).
//
// Соединение: SASL/SCRAM-SHA-512 поверх TLS (порт 9091).
// Аутентификация: пользователь "consumer", пароль из env KAFKA_CONSUMER_PASSWORD.
// TLS: CA-сертификат Яндекса из env KAFKA_CA_CERT.
// Брокеры: comma-separated FQDN в env KAFKA_BROKERS.
// Топик: env KAFKA_TOPIC (default: events).
// Consumer Group: env KAFKA_GROUP (default: my-consumer-group).
//
// Consumer group позволяет запустить несколько экземпляров consumer — Kafka
// автоматически распределит партиции между ними (load balancing).
package main

import (
	"context"     // Контекст для graceful shutdown (отмена чтения по сигналу)
	"crypto/tls"  // TLS-соединение
	"crypto/x509" // Пул доверенных CA
	"log"         // Вывод сообщений
	"os"          // Переменные окружения
	"os/signal"   // Перехват сигналов ОС (SIGINT, SIGTERM)
	"strings"     // Разбивка строки брокеров
	"syscall"     // Константы сигналов

	"github.com/IBM/sarama"                                    // Kafka-клиент
	kafkascram "github.com/an042/kafka-practical-work-7/scram" // Наш SCRAM-адаптер
)

func main() {
	// ─── Конфигурация из переменных окружения ─────────────────────────────────

	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		log.Fatal("KAFKA_BROKERS не задан")
	}
	brokers := strings.Split(brokersEnv, ",") // Разбиваем строку в срез

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "events" // Дефолтный топик
	}

	group := os.Getenv("KAFKA_GROUP")
	if group == "" {
		group = "my-consumer-group" // Consumer group ID — идентификатор группы
	}

	password := os.Getenv("KAFKA_CONSUMER_PASSWORD")
	if password == "" {
		log.Fatal("KAFKA_CONSUMER_PASSWORD не задан")
	}

	caFile := os.Getenv("KAFKA_CA_CERT")
	if caFile == "" {
		caFile = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
	}

	// ─── TLS-конфигурация ─────────────────────────────────────────────────────

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Не удалось прочитать CA-сертификат %s: %v", caFile, err)
	}

	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(caCert)
	if !ok {
		log.Fatal("Не удалось добавить CA-сертификат в пул")
	}

	tlsCfg := &tls.Config{
		RootCAs: caCertPool, // Доверяем только CA Яндекса
	}

	// ─── Sarama конфигурация ──────────────────────────────────────────────────

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_3_0_0 // Версия протокола Kafka

	// ─── SASL/SCRAM-SHA-512 ───────────────────────────────────────────────────
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	cfg.Net.SASL.User = "consumer"  // Пользователь consumer (только чтение из events)
	cfg.Net.SASL.Password = password
	cfg.Net.SASL.Handshake = true
	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &kafkascram.XDGSCRAMClient{HashGeneratorFcn: kafkascram.SHA512}
	}

	// ─── TLS ──────────────────────────────────────────────────────────────────
	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = tlsCfg

	// ─── Consumer group настройки ─────────────────────────────────────────────
	// OffsetNewest — читать только новые сообщения (с момента старта consumer).
	// Для чтения всей истории используй sarama.OffsetOldest.
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	// AutoCommit — автоматически коммитить смещения каждые 1 секунду.
	// Это означает «at-least-once» семантику: при краше возможна повторная обработка.
	cfg.Consumer.Offsets.AutoCommit.Enable = true

	// ─── Context для graceful shutdown ───────────────────────────────────────

	// context.WithCancel создаёт контекст с функцией отмены.
	// Когда вызовем cancel() — все методы с этим контекстом вернут ctx.Err().
	ctx, cancel := context.WithCancel(context.Background())

	// Канал для перехвата сигналов ОС (Ctrl+C = SIGINT, kill = SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Горутина: ждёт сигнал и отменяет контекст
	go func() {
		sig := <-sigChan              // Блокируемся до получения сигнала
		log.Printf("Получен сигнал %v, завершаем работу...", sig)
		cancel() // Отменяем контекст → ConsumeClaim завершится
	}()

	// ─── Создание Consumer Group ─────────────────────────────────────────────

	// Consumer Group — группа consumer'ов, совместно читающих из топика.
	// Kafka распределяет партиции между членами группы автоматически.
	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, cfg)
	if err != nil {
		log.Fatalf("Не удалось создать consumer group: %v", err)
	}
	defer consumerGroup.Close() // Освобождаем ресурсы при завершении

	// Наш обработчик сообщений (реализует sarama.ConsumerGroupHandler)
	handler := &consumerHandler{}

	log.Printf("Consumer запущен. Группа: %q, топик: %q. Ctrl+C для выхода.", group, topic)

	// ─── Цикл чтения ─────────────────────────────────────────────────────────
	// Consume — подключается к брокерам, получает назначенные партиции,
	// вызывает handler.ConsumeClaim() для каждой партиции.
	// При rebalance (добавление/удаление consumer) цикл рестартует.
	for {
		// topics — список топиков для чтения (можно читать из нескольких сразу)
		err := consumerGroup.Consume(ctx, []string{topic}, handler)
		if err != nil {
			log.Printf("Ошибка consume: %v", err)
		}
		// Проверяем, не отменён ли контекст (Ctrl+C)
		if ctx.Err() != nil {
			break // Выходим из цикла при отмене контекста
		}
	}

	log.Println("Consumer завершил работу.")
}

// consumerHandler — реализует интерфейс sarama.ConsumerGroupHandler.
// sarama вызывает методы этого интерфейса при управлении группой.
type consumerHandler struct{}

// Setup — вызывается перед началом чтения, после вступления в группу.
// Здесь можно инициализировать ресурсы (например, открыть DB-соединение).
func (h *consumerHandler) Setup(sess sarama.ConsumerGroupSession) error {
	log.Printf("Consumer group setup: member=%s, generation=%d",
		sess.MemberID(),    // Уникальный ID этого экземпляра consumer
		sess.GenerationID(), // Номер поколения (увеличивается при каждом rebalance)
	)
	return nil
}

// Cleanup — вызывается после завершения чтения (rebalance или остановка).
// Здесь освобождаем ресурсы инициализированные в Setup.
func (h *consumerHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	log.Println("Consumer group cleanup")
	return nil
}

// ConsumeClaim — основной метод обработки сообщений.
// Вызывается sarama для каждой партиции, назначенной этому consumer.
// claim содержит информацию о топике и партиции, а также канал сообщений.
func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("Начинаю читать: топик=%s, партиция=%d, начальный offset=%d",
		claim.Topic(),          // Имя топика
		claim.Partition(),      // Номер партиции (0, 1 или 2)
		claim.InitialOffset(),  // Offset с которого начинаем
	)

	// claim.Messages() — канал входящих сообщений для этой партиции.
	// Цикл завершится когда канал закроется (context отменён или rebalance).
	for msg := range claim.Messages() {
		// Выводим полученное сообщение
		log.Printf("Получено: partition=%d offset=%d key=%s value=%s",
			msg.Partition, // Номер партиции
			msg.Offset,    // Позиция сообщения в партиции
			string(msg.Key),   // Ключ сообщения
			string(msg.Value), // Тело сообщения
		)

		// MarkMessage — помечаем сообщение как обработанное.
		// sarama закоммитит этот offset при следующем AutoCommit (или при Commit()).
		// Если не вызвать — offset не сохранится и при перезапуске сообщение прочитается снова.
		sess.MarkMessage(msg, "") // "" — metadata (не используем)
	}

	return nil
}
