package component

import (
	"context"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"log"
)

var l *zap.SugaredLogger

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}
	l = logger.Sugar()
}

type Producer interface {
	Produce(content string) error
	Destroy()
}

type KafkaProducer struct {
	address []string
	topic   string
	writer  *kafka.Writer
}

func (k *KafkaProducer) initialize() {
	k.writer = &kafka.Writer{
		Addr:     kafka.TCP(k.address...),
		Topic:    k.topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func NewKafkaProducer(address string, topic string) *KafkaProducer {
	producer := &KafkaProducer{
		address: []string{address},
		topic:   topic,
	}
	producer.initialize()
	return producer
}

func (k *KafkaProducer) Produce(content string) error {
	err := k.writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte(content),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	return err
}

func (k *KafkaProducer) Close() {
	if k.writer != nil {
		err := k.writer.Close()
		if err != nil {
			l.Warnf("got error on closing writer: %v", k.writer)
		}
	}
}
