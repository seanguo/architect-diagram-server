package component

import (
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

type KafkaAdmin struct {
}

func (*KafkaAdmin) createTopic(topic string) error {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	defer conn.Close()
	if err != nil {
		panic(err.Error())
	}
	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
	return err
}
