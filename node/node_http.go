package node

import (
	"errors"
	"fmt"
	"strconv"

	"com.architectdiagram/m/component"
)

const (
	DEFAULT_HTTP_SERVER_PORT    = 8081
	DEFAULT_HTTP_SERVER_ADDRESS = "http://localhost"
)

var nextHttpServerPort int = DEFAULT_HTTP_SERVER_PORT

func init() {
	nodeFactories[REST_PRODUCER] = NewRestProducer
	nodeFactories[REST_SERVER] = NewRestServer
}

type RestProducer struct {
	BaseNode
	client *component.HttpClient
}

func NewRestProducer(t Type, id string) Node {
	return &RestProducer{
		BaseNode: BaseNode{
			ID:             id,
			eventListeners: make([]NodeEventListener, 0),
		},
	}
}

func (k *RestProducer) Initialize() {

}

func (k *RestProducer) Start() {

}

func (k *RestProducer) Stop() {

}
func (k *RestProducer) OnEvent(event string, eventSource string) {
	l.Infof("got event %s from source: %s", event, eventSource)
	k.Execute()
}
func (k *RestProducer) Execute() error {
	l.Infof("producer is executed")
	if k.client == nil {
		return errors.New("producer is not connected to server")
	}
	l.Infof("producer is producing")
	return k.client.Produce(k.generateMessage())
}

func (k *RestProducer) Update(propName, propValue string) {
}

func (k *RestProducer) OnConnect(node Node) error {
	var err error
	if k.client != nil {
		err = errors.New("producer is already connected")
		l.Errorf("%s", err)
		return err
	}
	ks, ok := node.(*RestServer)
	if ok {
		if !ks.started {
			ks.Start()
		}
		k.client = component.NewHttpClient(DEFAULT_HTTP_SERVER_ADDRESS + ":" + strconv.Itoa(ks.port))
		l.Infof("producer connected to server %s", DEFAULT_HTTP_SERVER_ADDRESS+":"+strconv.Itoa(ks.port))
	} else {
		err = fmt.Errorf("can't connect Rest producer to %v", ks)
		l.Errorf("%s", err)
	}
	return err
}

type RestServer struct {
	BaseNode
	started bool
	port    int
}

func NewRestServer(t Type, id string) Node {
	nextHttpServerPort++
	return &RestServer{
		BaseNode: BaseNode{
			ID:             id,
			eventListeners: make([]NodeEventListener, 0),
		},
		port: nextHttpServerPort,
	}
}

func (k *RestServer) Initialize() {

}

func (k *RestServer) Start() {
	if k.started {
		return
	}
	l.Infof("starting Rest server")
	k.started = true
	go func() {
		component.NewHttpServer(k.port, func(body []byte) {
			msg := string(body)
			l.Infof("Rest Server[%s] consumed: %s", k.ID, msg)
			for _, listener := range k.eventListeners {
				l.Infof("consumer[%s] is notifying listener[%v]", k.ID, listener)
				listener.OnEvent(msg, k.ID)
			}
		})
	}()
}

func (k *RestServer) Stop() {
}

func (k *RestServer) Execute() error {
	return nil
}

func (k *RestServer) OnConnect(node Node) error {
	switch node.(type) {
	case *RestProducer:
		err := node.OnConnect(k)
		if err != nil {
			return err
		}
		k.Start()
	case *KafkaServer:
		// create Kafka producer node
		producer := NewKafkaProducer(KAFKA_PRODUCER, "pair-"+k.GetID()).(*KafkaProducer)
		// add node as event listner
		k.AddEventListener(producer)
		k.pair = producer
		// connect producer to Kafka server
		producer.OnConnect(node)
	default:
		return fmt.Errorf("upported node type: %v", node)
	}
	return nil
}

func (k *RestServer) Update(propName, propValue string) {
}
