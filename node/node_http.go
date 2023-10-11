package node

import (
	"errors"
	"fmt"

	"com.architectdiagram/m/component"
)

const (
	DEFAULT_HTTP_SERVER_PORT    = 8081
	DEFAULT_HTTP_SERVER_ADDRESS = "http://localhost:8081"
)

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

func (k *RestProducer) Execute() error {
	l.Infof("producer is executed")
	if k.client == nil {
		return errors.New("producer is not connected to server")
	}
	l.Infof("producer is producing")
	return k.client.Produce("test1")
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
		k.client = component.NewHttpClient(DEFAULT_HTTP_SERVER_ADDRESS)
		l.Infof("producer connected to server %s", DEFAULT_HTTP_SERVER_ADDRESS)
	} else {
		err = fmt.Errorf("can't connect Rest producer to %v", ks)
		l.Errorf("%s", err)
	}
	return err
}

type RestServer struct {
	BaseNode
	started bool
}

func NewRestServer(t Type, id string) Node {
	return &RestServer{
		BaseNode: BaseNode{
			ID:             id,
			eventListeners: make([]NodeEventListener, 0),
		},
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
		component.NewHttpServer(DEFAULT_HTTP_SERVER_PORT, func(body []byte) {
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
		return nil
	default:
		return fmt.Errorf("upported node type: %v", node)
	}
}

func (k *RestServer) Update(propName, propValue string) {
}
