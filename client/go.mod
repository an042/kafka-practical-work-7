// go.mod — описание Go-модуля и его зависимостей.
//
// Модуль: github.com/an042/kafka-practical-work-7
//
// Основные зависимости:
//   - IBM/sarama: Go-клиент для Apache Kafka (поддерживает SASL/SCRAM-SHA-512, TLS)
//   - xdg-go/scram: реализация протокола SCRAM (требуется sarama, не входит в stdlib)
//
// После клонирования репозитория выполни:
//   cd client && go mod tidy
// go mod tidy скачает все транзитивные зависимости и создаст go.sum.
module github.com/an042/kafka-practical-work-7

go 1.21

require (
	github.com/IBM/sarama v1.43.3
	github.com/xdg-go/scram v1.1.2
)
