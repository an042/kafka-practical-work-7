// Пакет scram — адаптер SCRAM-SHA-512 для библиотеки sarama.
//
// ВАЖНО: это то самое место, о котором предупреждают лекции.
// IBM/sarama не включает реализацию SCRAM «из коробки» — в отличие от Python
// (kafka-python) и Java (kafka-clients), где SCRAM встроен. В Go нужно:
//   1. Подключить внешний пакет github.com/xdg-go/scram
//   2. Написать тип, реализующий интерфейс sarama.SCRAMClient
//   3. Передать генератор этого типа через SCRAMClientGeneratorFunc при настройке конфига
//
// Без этого пакета sarama падает с ошибкой "SCRAM mechanism not found".
package scram

import (
	"crypto/sha256" // SHA-256 — для SCRAM-SHA-256 (если потребуется)
	"crypto/sha512" // SHA-512 — для SCRAM-SHA-512 (требуется Yandex Cloud)
	"hash"          // Интерфейс hash.Hash — общий тип для всех хеш-функций

	"github.com/IBM/sarama"    // Kafka-клиент — определяет интерфейс SCRAMClient
	"github.com/xdg-go/scram" // Реализация протокола SCRAM (RFC 5802)
)

// SHA256 — генератор хеш-функции SHA-256.
// Используется для SCRAM-SHA-256 (менее безопасен, чем SHA-512).
var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }

// SHA512 — генератор хеш-функции SHA-512.
// Используется для SCRAM-SHA-512 — обязательный механизм в Yandex Cloud Kafka.
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

// XDGSCRAMClient — адаптер между sarama.SCRAMClient и xdg-go/scram.
// sarama при установке соединения вызывает методы в порядке: Begin → Step(ы) → Done.
type XDGSCRAMClient struct {
	*scram.Client             // Хранит учётные данные (логин/пароль) и алгоритм хеширования
	*scram.ClientConversation // Управляет обменом challenge/response в текущей сессии
	scram.HashGeneratorFcn    // Функция создания хеш-объекта (SHA256 или SHA512)
}

// Begin — инициализация клиента перед SASL-рукопожатием.
// Вызывается один раз на каждое новое соединение с брокером.
// userName, password — учётные данные пользователя Kafka (producer или consumer).
// authzID — идентификатор авторизации, обычно "" (пустой = совпадает с userName).
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) error {
	var err error
	// Создаём SCRAM-клиент с нужным хеш-алгоритмом и учётными данными
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err // Вернём ошибку в sarama — соединение не установится
	}
	// Начинаем новый «разговор» (conversation) — объект отслеживает состояние обмена
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step — один шаг SCRAM-обмена challenge/response.
// Вызывается несколько раз: первый раз с пустой строкой (клиент начинает),
// затем с challenge от брокера, пока Done() не вернёт true.
// Возвращает строку-ответ для отправки брокеру.
func (x *XDGSCRAMClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

// Done — возвращает true, когда SCRAM-аутентификация успешно завершена.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// Статическая проверка совместимости типов во время компиляции.
// Если убрать любой из методов выше — получим ошибку здесь, а не в рантайме.
var _ sarama.SCRAMClient = &XDGSCRAMClient{}
