package main

import (
	"ccrayz/exmaple-kafka/consumer"
	"ccrayz/exmaple-kafka/producer"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	args := os.Args[1:]
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	bootstrapServers := os.Getenv("BOOTSTARP_SERVERS")
	consumerGroupID := os.Getenv("CONSUMER_GROUP_ID")
	topic := os.Getenv("TOPIC")

	fmt.Println("Command-line arguments are:")
	for _, arg := range args {
		fmt.Println(arg)
	}

	if args[0] == "-p" {
		fmt.Println("Producer started")
		producer.Start(bootstrapServers, topic)
	} else if args[0] == "-c" {
		fmt.Println("Consumer started")
		consumer.Start(bootstrapServers, consumerGroupID, topic)
	}
}
