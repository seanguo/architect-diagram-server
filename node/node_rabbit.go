package node

import (
	"errors"
	"fmt"

	"com.architectdiagram/m/component"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DEFAULT_RABBIT_MQ_ADDRESS  string = "amqp://guest:guest@localhost:5672/"
	DEFAULT_RABBIT_MQ_EXCHANGE string = ""
	// DEFAULT_RABBIT_MQ_CONSUMER_GROUP string = "arc_consumer_group"
)

func init() {
	nodeFactories[RABBIT_PRODUCER] = NewRabbitProducer
	nodeFactories[RABBIT_SERVER] = NewRabbitServer
	nodeFactories[RABBIT_CONSUMER] = NewRabbitConsumer
}

type RabbitProducer struct {
	BaseNode
	producer *component.RabbitMQClient
}

func NewRabbitProducer(t Type, id string) Node {
	return &RabbitProducer{
		BaseNode: BaseNode{
			ID:             id,
			eventListeners: make([]NodeEventListener, 0),
		},
	}
}

func (k *RabbitProducer) Initialize() {

}

func (k *RabbitProducer) Start() {

}

func (k *RabbitProducer) Stop() {
	if k.producer != nil {
		k.producer.Close()
	}
}

func (k *RabbitProducer) Execute() error {
	l.Infof("producer is executed")
	if k.producer == nil {
		return errors.New("producer is not connected to server")
	}
	l.Infof("producer is producing")
	return k.producer.Produce("test1")
}

func (k *RabbitProducer) Update(propName, propValue string) {
}

func (k *RabbitProducer) OnConnect(node Node) error {
	var err error
	if k.producer != nil {
		err = errors.New("producer is already connected")
		l.Errorf("%s", err)
		return err
	}
	ks, ok := node.(*RabbitServer)
	if ok {
		k.producer = component.NewRabbitMQClient(DEFAULT_RABBIT_MQ_ADDRESS, DEFAULT_RABBIT_MQ_EXCHANGE)
		l.Infof("producer connected to server %s of exchange %s", DEFAULT_RABBIT_MQ_ADDRESS, DEFAULT_RABBIT_MQ_EXCHANGE)
	} else {
		err = fmt.Errorf("can't connect Rabbit producer to %v", ks)
		l.Errorf("%s", err)
	}
	return err
}

type RabbitConsumer struct {
	BaseNode
	consumer *component.RabbitMQClient
	messages <-chan amqp.Delivery
	done     chan bool
	shutdown chan bool
	props    map[string]string
}

func NewRabbitConsumer(t Type, id string) Node {
	consumer := &RabbitConsumer{
		BaseNode: BaseNode{
			ID:             id,
			eventListeners: make([]NodeEventListener, 0),
		},
		done:     make(chan bool, 1),
		shutdown: make(chan bool),
		props:    make(map[string]string),
	}
	return consumer
}

func (k *RabbitConsumer) Initialize() {
}

func (k *RabbitConsumer) Start() {
	k.messages = k.consumer.Consume()
	go func() {
		for {
			select {
			case msg := <-k.messages:
				if msg.Body != nil && len(msg.Body) > 0 {
					l.Infof("Consumer[%s] consumed: %s", k.ID, msg)
					for _, listener := range k.eventListeners {
						l.Infof("consumer[%s] is notifying listener[%v]", k.ID, listener)
						listener.OnEvent(string(msg.Body), k.ID)
					}
				}

			case <-k.done:
				l.Infof("Consumer[%s] is quitting", k.ID)
				k.shutdown <- true
			}
		}
	}()
	l.Infof("conumser[%s] is started", k.ID)
}

func (k *RabbitConsumer) Stop() {
	l.Infof("consumer[%s] is stopping", k.ID)
	k.done <- true
	k.consumer.Close()
	<-k.shutdown
	l.Infof("consumer[%s] is stopped", k.ID)
}

func (k *RabbitConsumer) Execute() error {
	return nil
}

func (k *RabbitConsumer) Update(propName, propValue string) {
	l.Infof("consumer is updating node[%s]'s %s to %s", k.ID, propName, propValue)
}

func (k *RabbitConsumer) OnConnect(node Node) error {
	if k.consumer != nil {
		err := errors.New("consumer is already connected")
		l.Errorf("%s", err)
		return err
	}
	ks, ok := node.(*RabbitServer)
	if ok {
		k.consumer = component.NewRabbitMQClient(DEFAULT_RABBIT_MQ_ADDRESS, DEFAULT_RABBIT_MQ_EXCHANGE)
		l.Infof("consumer connected to server %s of exchnage %s", DEFAULT_RABBIT_MQ_ADDRESS, DEFAULT_RABBIT_MQ_EXCHANGE)
		k.Start()
	} else {
		err := fmt.Errorf("can't connect Rabbit consumer to %v", ks)
		l.Errorf("%s", err)
		return err
	}
	return nil
}

type RabbitServer struct {
	BaseNode
}

func NewRabbitServer(t Type, id string) Node {
	return &RabbitServer{
		BaseNode: BaseNode{
			ID:             id,
			eventListeners: make([]NodeEventListener, 0),
		},
	}
}

func (k *RabbitServer) Initialize() {

}

func (k *RabbitServer) Start() {

}

func (k *RabbitServer) Stop() {
}

func (k *RabbitServer) Execute() error {
	return nil
}

func (k *RabbitServer) OnConnect(node Node) error {
	switch node.(type) {
	case *RabbitProducer, *RabbitConsumer:
		return node.OnConnect(k)
	default:
		return fmt.Errorf("upported node type: %v", node)
	}
}

func (k *RabbitServer) Update(propName, propValue string) {
}
