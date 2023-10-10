package node

import (
	"errors"
	"fmt"

	"com.architectdiagram/m/component"
)

const (
	DEFAULT_KAFKA_ADDRESS        string = "localhost:9092"
	DEFAULT_KAFKA_TOPIC          string = "arc_diagrams"
	DEFAULT_KAFKA_CONSUMER_GROUP string = "arc_consumer_group"
)

func init() {
	nodeFactories[KAFKA_PRODUCER] = NewKafkaProducer
	nodeFactories[KAFKA_CONSUMER] = NewKafkaConsumer
	nodeFactories[KAFKA_SERVER] = NewKafkaServer
}

type KafkaProducer struct {
	BaseNode
	producer *component.KafkaProducer
}

func NewKafkaProducer(t Type, id string) Node {
	return &KafkaProducer{
		BaseNode: BaseNode{
			ID:             id,
			eventListeners: make([]NodeEventListener, 0),
		},
	}
}

func (k *KafkaProducer) Initialize() {

}

func (k *KafkaProducer) Start() {

}

func (k *KafkaProducer) Stop() {
	if k.producer != nil {
		k.producer.Close()
	}
}

func (k *KafkaProducer) Execute() error {
	l.Infof("producer is executed")
	if k.producer == nil {
		return errors.New("producer is not connected to server")
	}
	l.Infof("producer is producing")
	return k.producer.Produce("test1")
}

func (k *KafkaProducer) Update(propName, propValue string) {
}

func (k *KafkaProducer) OnConnect(node Node) error {
	var err error
	if k.producer != nil {
		err = errors.New("producer is already connected")
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
	shutdown chan bool
	props    map[string]string
}

func NewKafkaConsumer(t Type, id string) Node {
	consumer := &KafkaConsumer{
		BaseNode: BaseNode{
			ID:             id,
			eventListeners: make([]NodeEventListener, 0),
		},
		// messages: make(chan string, 1),
		done:     make(chan bool, 1),
		shutdown: make(chan bool),
		props:    make(map[string]string),
	}
	consumer.props["consumerGroup"] = DEFAULT_KAFKA_CONSUMER_GROUP
	return consumer
}

func (k *KafkaConsumer) Initialize() {
}

func (k *KafkaConsumer) Start() {
	k.messages = make(chan string, 1)
	go func() {
		for msg := range k.messages {
			l.Infof("Consumer[%s] consumed: %s", k.ID, msg)
			for _, listener := range k.eventListeners {
				l.Infof("consumer[%s] is notifying listener[%v]", k.ID, listener)
				listener.OnEvent(msg, k.ID)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-k.done:
				l.Infof("Consumer[%s] is quitting", k.ID)
				close(k.messages)
				k.shutdown <- true
				return
			default:
				k.messages <- k.consumer.Consume()
			}
		}
	}()
	l.Infof("conumser[%s] is started", k.ID)
}

func (k *KafkaConsumer) Stop() {
	l.Infof("consumer[%s] is stopping", k.ID)
	k.done <- true
	k.consumer.Close()
	<-k.shutdown
	l.Infof("consumer[%s] is stopped", k.ID)
}

func (k *KafkaConsumer) Execute() error {
	return nil
}

func (k *KafkaConsumer) Update(propName, propValue string) {
	l.Infof("consumer is updating node[%s]'s %s to %s", k.ID, propName, propValue)
	if k.consumer != nil {
		k.Stop()
		k.consumer = component.NewKafkaConsumer(DEFAULT_KAFKA_ADDRESS, DEFAULT_KAFKA_TOPIC, propValue)
		l.Infof("consumer reconnected to server %s of topic %s", DEFAULT_KAFKA_ADDRESS, DEFAULT_KAFKA_TOPIC)
		k.Start()
	}
	k.props[propName] = propValue
}

func (k *KafkaConsumer) OnConnect(node Node) error {
	if k.consumer != nil {
		err := errors.New("consumer is already connected")
		l.Errorf("%s", err)
		return err
	}
	ks, ok := node.(*KafkaServer)
	if ok {
		k.consumer = component.NewKafkaConsumer(DEFAULT_KAFKA_ADDRESS, DEFAULT_KAFKA_TOPIC, k.props["consumerGroup"])
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

func NewKafkaServer(t Type, id string) Node {
	return &KafkaServer{
		BaseNode: BaseNode{
			ID:             id,
			eventListeners: make([]NodeEventListener, 0),
		},
	}
}

func (k *KafkaServer) Initialize() {

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

func (k *KafkaServer) Update(propName, propValue string) {
}