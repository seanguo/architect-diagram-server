package component

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Consume() string
	Destroy()
}

type KafkaConsumer struct {
	address []string
	topic   string
	reader  *kafka.Reader
}

func (k *KafkaConsumer) initialize() {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	k.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   k.address,
		Topic:     k.topic,
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	//r.SetOffset(42)
}

func NewKafkaConsumer(address string, topic string) *KafkaConsumer {
	consumer := &KafkaConsumer{
		address: []string{address},
		topic:   topic,
	}
	consumer.initialize()
	return consumer
}

func (k *KafkaConsumer) Consume() string {
	m, err := k.reader.ReadMessage(context.Background())
	if err != nil {
		l.Errorf("error on consuming message: %s", err)
	}
	fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	return string(m.Value)
}

func (k *KafkaConsumer) Close() {
	if k.reader != nil {
		err := k.reader.Close()
		if err != nil {
			l.Warnf("got error on closing reader: %v", k.reader)
		}
	}
}
