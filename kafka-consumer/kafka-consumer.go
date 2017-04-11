package kafkaconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/cloudfoundry/sonde-go/events"
)

type KafkaConsumer struct {
	KConsumer      *cluster.Consumer
	ChEvents       <-chan *events.Envelope
	ChErrors       <-chan error
	ChNotification <-chan *cluster.Notification
}

func NewKafaConsumer(brokers string, topicsList string) *KafkaConsumer {

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// init consumer
	kafkaBrokers := strings.Split(brokers, ",")
	topics := strings.Split(topicsList, ",")
	consumer, err := cluster.NewConsumer(kafkaBrokers, "my-consumer-group", topics, config)
	if err != nil {
		panic(err)
	}

	return &KafkaConsumer{
		KConsumer:      consumer,
		ChErrors:       consumer.Errors(),
		ChNotification: consumer.Notifications(),
	}
}

func (k *KafkaConsumer) Consume(ctx context.Context) {

	outputs := make(chan *events.Envelope)

	// consume messages
	go func() {
		for {
			select {
			case msg, more := <-k.KConsumer.Messages():
				if more {
					var eve events.Envelope
					err := json.Unmarshal(msg.Value, &eve)
					if err != nil {
						fmt.Println(err)
					} else {
						outputs <- &eve
					}
					k.KConsumer.MarkOffset(msg, "") // mark message as processed
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	k.ChEvents = outputs

}

//Let's Close the Kafka Consumer
func (k *KafkaConsumer) Close() error {
	return k.KConsumer.Close()
}
