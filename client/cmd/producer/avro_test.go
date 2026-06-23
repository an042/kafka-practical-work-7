// avro_test.go — проверяет Avro-кодирование и Confluent Wire Format без Kafka.
package main

import (
	"bytes"           // Сравнение байтовых срезов
	"encoding/binary" // Чтение schema_id из заголовка
	"testing"         // Стандартный пакет тестирования Go

	goavro "github.com/linkedin/goavro/v2" // Avro-кодек
)

// TestAvroCodecCreation проверяет, что схема парсится без ошибок.
func TestAvroCodecCreation(t *testing.T) {
	_, err := goavro.NewCodec(avroSchema) // avroSchema — константа из main.go
	if err != nil {
		t.Fatalf("Не удалось создать Avro-кодек из схемы: %v", err)
	}
}

// TestAvroEncodeDecodeRoundtrip кодирует Event и декодирует обратно,
// проверяя что все поля сохранились без потерь.
func TestAvroEncodeDecodeRoundtrip(t *testing.T) {
	codec, _ := goavro.NewCodec(avroSchema)

	// Исходные данные — такие же, как отправляет producer.
	// id передаётся как int, но goavro декодирует Avro int обратно как int32.
	original := map[string]interface{}{
		"id":      42,
		"ts":      "2026-06-23T12:00:00+03:00",
		"source":  "go-producer",
		"payload": map[string]interface{}{"null": nil}, // union → null
	}

	// Кодируем в Avro binary
	encoded, err := codec.BinaryFromNative(nil, original)
	if err != nil {
		t.Fatalf("BinaryFromNative вернул ошибку: %v", err)
	}

	// Декодируем обратно
	decoded, _, err := codec.NativeFromBinary(encoded)
	if err != nil {
		t.Fatalf("NativeFromBinary вернул ошибку: %v", err)
	}

	// Проверяем каждое поле
	rec := decoded.(map[string]interface{})

	// goavro декодирует Avro int (32-bit) → int32, поэтому сравниваем через int32
	if rec["id"] != int32(42) {
		t.Errorf("id: ожидали int32(42), получили %T(%v)", rec["id"], rec["id"])
	}
	if rec["ts"] != original["ts"] {
		t.Errorf("ts: ожидали %v, получили %v", original["ts"], rec["ts"])
	}
	if rec["source"] != original["source"] {
		t.Errorf("source: ожидали %v, получили %v", original["source"], rec["source"])
	}
}

// TestConfluentWireFormat проверяет структуру итогового сообщения:
// [0x00][schema_id 4 байта big-endian][avro payload].
func TestConfluentWireFormat(t *testing.T) {
	codec, _ := goavro.NewCodec(avroSchema)

	native := map[string]interface{}{
		"id":      1,
		"ts":      "2026-06-23T12:00:00+03:00",
		"source":  "go-producer",
		"payload": map[string]interface{}{"null": nil},
	}

	avroBinary, _ := codec.BinaryFromNative(nil, native)

	schemaID := int32(1)
	var buf bytes.Buffer
	buf.WriteByte(0x00)                                 // Magic byte
	_ = binary.Write(&buf, binary.BigEndian, schemaID) // Schema ID
	buf.Write(avroBinary)                               // Avro payload

	msg := buf.Bytes()

	// Проверяем magic byte
	if msg[0] != 0x00 {
		t.Errorf("Magic byte: ожидали 0x00, получили 0x%02x", msg[0])
	}

	// Проверяем schema_id
	gotID := int32(binary.BigEndian.Uint32(msg[1:5]))
	if gotID != schemaID {
		t.Errorf("Schema ID: ожидали %d, получили %d", schemaID, gotID)
	}

	// Проверяем что payload декодируется
	avroPayload := msg[5:]
	decoded, _, err := codec.NativeFromBinary(avroPayload)
	if err != nil {
		t.Fatalf("Avro payload в Wire Format не декодируется: %v", err)
	}

	rec := decoded.(map[string]interface{})
	// goavro декодирует Avro int → int32
	if rec["id"] != int32(1) {
		t.Errorf("После Wire Format round-trip: id %T(%v) != int32(1)", rec["id"], rec["id"])
	}

	t.Logf("Wire Format OK: %d байт (1 magic + 4 schema_id + %d avro)", len(msg), len(avroPayload))
}

// TestAvroPayloadUnionString проверяет поле payload с реальной строкой (не null).
func TestAvroPayloadUnionString(t *testing.T) {
	codec, _ := goavro.NewCodec(avroSchema)

	native := map[string]interface{}{
		"id":      2,
		"ts":      "2026-06-23T12:00:00+03:00",
		"source":  "go-producer",
		"payload": map[string]interface{}{"string": "test data"}, // union → string
	}

	encoded, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		t.Fatalf("Ошибка кодирования с payload=string: %v", err)
	}

	decoded, _, err := codec.NativeFromBinary(encoded)
	if err != nil {
		t.Fatalf("Ошибка декодирования с payload=string: %v", err)
	}

	rec := decoded.(map[string]interface{})
	payloadUnion, ok := rec["payload"].(map[string]interface{})
	if !ok {
		t.Fatalf("payload не является map: %T", rec["payload"])
	}
	if payloadUnion["string"] != "test data" {
		t.Errorf("payload.string: ожидали 'test data', получили %v", payloadUnion["string"])
	}
}
