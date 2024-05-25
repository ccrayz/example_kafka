package topics

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func WikiMedia(p *kafka.Producer) {
	topic := "wikimedia"

	const url = "https://stream.wikimedia.org/v2/stream/recentchange"

	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error getting stream: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			go func(data string) {
				msg := &kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          []byte(data),
					Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
				}

				err := p.Produce(msg, nil)
				if err != nil {
					fmt.Printf("Failed to produce message: %v\n", err)
				}
			}(line[5:])
		}
	}
}
