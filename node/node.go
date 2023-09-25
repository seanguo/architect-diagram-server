package node

import (
	"errors"
	"fmt"
	"log"

	"com.architectdiagram/m/component"
	"go.uber.org/zap"
)

var l *zap.SugaredLogger

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}
	l = logger.Sugar()
}

type Type string

const (
	KAFKA_PRODUCER Type = "kafka_producer"
	KAFKA_CONSUMER Type = "kafka_consumer"
	KAFKA_SERVER   Type = "kafka_server"
)
const (
	DEFAULT_KAFKA_ADDRESS string = "localhost:9092"
	DEFAULT_KAFKA_TOPIC   string = "arc_diagrams"
)

type Node interface {
	Start()
	Stop()
	OnConnect(node Node) error
	Execute() error
	GetID() string
}

type BaseNode struct {
	ID string `json:"id"`
}

func (k *BaseNode) GetID() string {
	return k.ID
}

type KafkaProducer struct {
	BaseNode
	producer *component.KafkaProducer
}

func (k *KafkaProducer) Start() {

}

func (k *KafkaProducer) Stop() {
	if k.producer != nil {
		k.producer.Close()
	}
}

func (k *KafkaProducer) Execute() error {
	if k.producer == nil {
		return errors.New("producer is not connected to server")
	}
	return k.producer.Produce("test1")
}

func (k *KafkaProducer) OnConnect(node Node) error {
	var err error
	if k.producer != nil {
		err = errors.New("Kafka producer is already connected")
		l.Errorf("%s", err)
		return err
	}
	ks, ok := node.(*KafkaServer)
	if ok {
		k.producer = component.NewKafkaProducer(DEFAULT_KAFKA_ADDRESS, DEFAULT_KAFKA_TOPIC)
		l.Infof("producer connected to server %s of topic %s", DEFAULT_KAFKA_ADDRESS, DEFAULT_KAFKA_TOPIC)
	} else {
		err = fmt.Errorf("can't connect Kafka producer to %v", ks)
		l.Errorf("%s", err)
	}
	return err
}

type KafkaConsumer struct {
	BaseNode
	consumer *component.KafkaConsumer
	messages chan string
	done     chan bool
}

func (k *KafkaConsumer) Start() {
	go func() {
		for msg := range k.messages {
			l.Infof("Consumer[%s] consumed: %s", k.ID, msg)
		}
	}()
	go func() {
		for {
			select {
			case <-k.done:
				l.Infof("Consumer[%s] is quitting", k.ID)
				close(k.messages)
				return
			default:
				k.messages <- k.consumer.Consume()
			}
		}
	}()
}

func (k *KafkaConsumer) Stop() {
	k.done <- true
}

func (k *KafkaConsumer) Execute() error {
	return nil
}

func (k *KafkaConsumer) OnConnect(node Node) error {
	if k.consumer != nil {
		err := errors.New("Kafka consumer is already connected")
		l.Errorf("%s", err)
		return err
	}
	ks, ok := node.(*KafkaServer)
	if ok {
		k.consumer = component.NewKafkaConsumer(DEFAULT_KAFKA_ADDRESS, DEFAULT_KAFKA_TOPIC)
		l.Infof("consumer connected to server %s of topic %s", DEFAULT_KAFKA_ADDRESS, DEFAULT_KAFKA_TOPIC)
		k.Start()
	} else {
		err := fmt.Errorf("can't connect Kafka consumer to %v", ks)
		l.Errorf("%s", err)
		return err
	}
	return nil
}

type KafkaServer struct {
	BaseNode
}

func (k *KafkaServer) Start() {

}

func (k *KafkaServer) Stop() {
}

func (k *KafkaServer) Execute() error {
	return nil
}

func (k *KafkaServer) OnConnect(node Node) error {
	switch node.(type) {
	case *KafkaProducer, *KafkaConsumer:
		return node.OnConnect(k)
	default:
		return fmt.Errorf("upported node type: %v", node)
	}
}

func New(t Type, id string) Node {
	switch t {
	case KAFKA_PRODUCER:
		return &KafkaProducer{
			BaseNode: BaseNode{
				ID: id,
			},
		}
	case KAFKA_CONSUMER:
		return &KafkaConsumer{
			BaseNode: BaseNode{
				ID: id,
			},
			messages: make(chan string, 1),
			done:     make(chan bool),
		}
	case KAFKA_SERVER:
		return &KafkaServer{
			BaseNode: BaseNode{
				ID: id,
			},
		}
	}
	return nil
}
