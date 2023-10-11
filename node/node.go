package node

import (
	"log"

	"go.uber.org/zap"
)

var l *zap.SugaredLogger
var nodeFactories map[Type]func(t Type, id string) Node

func init() {
	nodeFactories = make(map[Type]func(t Type, id string) Node)

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

	RABBIT_PRODUCER Type = "rabbit_producer"
	RABBIT_SERVER   Type = "rabbit_server"
	RABBIT_CONSUMER Type = "rabbit_consumer"

	REST_PRODUCER Type = "rest_producer"
	REST_SERVER   Type = "rest_server"
)

type Node interface {
	NodeEventSource
	Initialize()
	Start()
	Stop()
	OnConnect(node Node) error
	Execute() error
	GetID() string
	Update(propName, propValue string)
}

type NodeEventListener interface {
	OnEvent(event string, eventSource string)
}

type NodeEventSource interface {
	AddEventListener(listener NodeEventListener)
}

type BaseNode struct {
	ID             string `json:"id"`
	eventListeners []NodeEventListener
}

func (k *BaseNode) GetID() string {
	return k.ID
}

// func (k *BaseNode) Update(propName, propValue string) {
// 	l.Infof("updating node[%s]'s %s to %s", k.ID, propName, propValue)
// }

func (k *BaseNode) AddEventListener(listener NodeEventListener) {
	l.Infof("adding %v to node[%s]", listener, k.ID)
	k.eventListeners = append(k.eventListeners, listener)
}

func New(t Type, id string) Node {
	fac := nodeFactories[t]
	if fac == nil {
		l.Errorf("unsupported node type: %s", t)
		return nil
	}
	return fac(t, id)
}
