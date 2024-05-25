package topics

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

func ChaindataBlocknumber(p *kafka.Producer) {
	topic := "chaindata_blocknumber"

	client, err := ethclient.Dial(os.Getenv("ETH_WS_URL"))
	if err != nil {
		log.Fatalf("Failed to connect to the Ethereum client: %v", err)
	}

	headers := make(chan *types.Header)
	sub, err := client.SubscribeNewHead(context.Background(), headers)
	if err != nil {
		log.Fatalf("Failed to subscribe to new headers: %v", err)
	}

	fmt.Println("Started listening for new blocks...")
	for {
		select {
		case err := <-sub.Err():
			log.Fatal(err)

		case header := <-headers:
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(header.Number.String()),
				Headers:        []kafka.Header{{Key: "ETH", Value: []byte("header values are binary")}},
			}

			err := p.Produce(msg, nil)
			if err != nil {
				fmt.Printf("Failed to produce message: %v\n", err)
			}
		}
		p.Flush(15 * 1000)
	}
}
