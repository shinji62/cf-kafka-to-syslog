package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/cloudfoundry/sonde-go/events"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	//	cliArgs       = kingpin.New("CF Kafka Consumer to Syslog", "help")
	kfBrokersList = kingpin.Flag("kafka-brokers", "Kafka broker list comma separated").Required().String()
	topicsList    = kingpin.Flag("topics", "topics list comma separated").Required().String()
)

// CLI is the command line object
type CLI struct {
	// outStream and errStream are the stdout and stderr
	// to write message from the CLI.
	outStream, errStream io.Writer
}

// Run invokes the CLI with the given arguments.
func (cli *CLI) Run() int {
	kingpin.Parse()

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// init consumer
	kafkaBrokers := strings.Split(*kfBrokersList, ",")
	topics := strings.Split(*topicsList, ",")
	consumer, err := cluster.NewConsumer(kafkaBrokers, "my-consumer-group", topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// Create a ctx for cancelation signal across the goroutine.
	ctx, cancel := context.WithCancel(context.Background())

	//let's take care of error from consumer
	go func() {
		for {
			select {
			case err, more := <-consumer.Errors():
				if more {
					log.Printf("Error: %s\n", err.Error())
				}
			case ntf, more := <-consumer.Notifications():
				if more {
					log.Printf("Rebalanced: %+v\n", ntf)
				}
			case <-ctx.Done():
				fmt.Println("Stopping Error routine")
				return
			}

		}
	}()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		<-signals
		fmt.Println("Receiving Signal let's stop all goroutine")
		cancel()
	}()

	// consume messages
	go func() {
		for {
			select {
			case msg, more := <-consumer.Messages():
				if more {
					//fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
					if msg.Topic == "LogMessage" {
						var eve events.Envelope
						err := json.Unmarshal(msg.Value, &eve)
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println(eve)
						}

					}

					consumer.MarkOffset(msg, "") // mark message as processed
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	<-ctx.Done()
	defer cancel()
	return 0
}
