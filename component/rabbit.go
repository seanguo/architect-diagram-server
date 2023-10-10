package component

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	address  []string
	exchange string
	queue    string
	conn     *amqp.Connection
}

func (p *RabbitMQClient) channel() (*amqp.Channel, error) {
	ch, err := p.conn.Channel()
	if err != nil {
		l.Errorf("failed to create channel: %s", err)
		return nil, err
	}
	return ch, nil
}

func (p *RabbitMQClient) declareQueue(ch *amqp.Channel) (amqp.Queue, error) {
	q, err := ch.QueueDeclare(
		p.queue, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		failOnError(err, "Failed to declare a queue")
		return q, err
	}
	return q, err
}

func (p *RabbitMQClient) Produce(content string) error {
	ch, err := p.channel()
	if err != nil {
		return nil
	}
	defer ch.Close()

	q, err := p.declareQueue(ch)
	if err != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		p.exchange, // exchange
		q.Name,     // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(content),
		})
	if err != nil {
		failOnError(err, "Failed to publish a message")
		return err
	}

	l.Infof(" [x] Sent %s\n", content)
	return nil
}

func failOnError(err error, msg string) {
	if err != nil {
		l.Errorf("%s: %s", msg, err)
	}
}

func (p *RabbitMQClient) Consume() <-chan amqp.Delivery {
	ch, err := p.channel()
	if err != nil {
		return nil
	}
	// defer ch.Close()

	q, err := p.declareQueue(ch)
	if err != nil {
		return nil
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	return msgs
}

func (p *RabbitMQClient) initialize() error {
	var err error
	p.conn, err = amqp.Dial(p.address[0])
	if err != nil {
		l.Errorf("failed to initialize connection to %s: %s", p.address[0], err)
	}
	return err
}

func NewRabbitMQClient(address string, exchange string) *RabbitMQClient {
	producer := &RabbitMQClient{
		address:  []string{address},
		exchange: exchange,
		queue:    "test",
	}
	err := producer.initialize()
	if err != nil {
		return nil
	}
	return producer
}

func (p *RabbitMQClient) Close() {
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			l.Warnf("got error on closing RabbitMQ connection: %v", p.conn)
		}
	}
}
