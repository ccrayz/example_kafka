package consumer

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func Start(bootstrapServers string, groupID string, topic string) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"broker.address.family":         "v4",
		"group.id":                      groupID,
		"session.timeout.ms":            6000,
		"auto.commit.interval.ms":       1000,
		"auto.offset.reset":             "earliest",
		"partition.assignment.strategy": "cooperative-sticky",
		// Whether or not we store offsets automatically.
		"enable.auto.offset.store": false,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	err = c.SubscribeTopics([]string{topic}, rebalanceCallback)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topics: %s", err))
	}

	fmt.Printf("Created Consumer %v\n", c)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(-1)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				go processMassage(e)
				_, err := c.StoreMessage(e)
				fmt.Println("store message", e.TopicPartition, err)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error storing offset after message %s:\n",
						e.TopicPartition)
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			case kafka.OffsetsCommitted:
				fmt.Printf("Offsets committed: %v\n", e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	// Graceful shutdown
	fmt.Println("gracefully shutdown consumer")
	fmt.Println("Waiting for consumer to close...")
	err = c.Close()

	if err != nil {
		fmt.Printf("Failed to close consumer: %s\n", err)
	} else {
		fmt.Println("Consumer closed successfully")
	}
}

func processMassage(e *kafka.Message) {
	// fmt.Printf("Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
	if e.Headers != nil {
		fmt.Printf("Headers: %v\n", e.Headers)
	}
}

func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		fmt.Printf("%% %s rebalance: %d new partition(s) assigned: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		err := c.IncrementalAssign(ev.Partitions)
		// err := c.Assign(ev.Partitions)
		if err != nil {
			return err
		}

	case kafka.RevokedPartitions:
		fmt.Printf("%% %s rebalance: %d partition(s) revoked: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		if c.AssignmentLost() {
			fmt.Fprintln(os.Stderr, "Assignment lost involuntarily, commit may fail")
		}

		commitedOffsets, err := c.Commit()

		if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
			fmt.Fprintf(os.Stderr, "Failed to commit offsets: %s\n", err)
			return err
		}
		fmt.Printf("%% Commited offsets to Kafka: %v\n", commitedOffsets)

	default:
		fmt.Fprintf(os.Stderr, "Unxpected event type: %v\n", event)
	}

	return nil
}
