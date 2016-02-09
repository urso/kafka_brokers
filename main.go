package main

import (
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

func main() {
	addr := os.Args[1]
	fmt.Println("try to connect to,", addr)

	broker := sarama.NewBroker(addr)
	err := broker.Open(nil)
	if err != nil {
		fmt.Println("failed to open broker: ", err)
		os.Exit(1)
	}

	defer broker.Close()

	var topics []string
	if len(os.Args) > 2 {
		topics = os.Args[2:]
		fmt.Println("query for topics: ", topics)
	}

	request := sarama.MetadataRequest{Topics: topics}
	response, err := broker.GetMetadata(&request)
	if err != nil {
		fmt.Println("metadata request failed with: ", err)
		os.Exit(1)
	}

	fmt.Println("known topics")
	for _, t := range response.Topics {
		fmt.Println("topic")
		fmt.Println("  name: ", t.Name)
		fmt.Println("  error: ", t.Err)
		fmt.Println("  partitions: ")
		for _, p := range t.Partitions {
			fmt.Println("    ", p.ID)
		}
	}

	fmt.Println("broker addresses")
	for _, b := range response.Brokers {
		fmt.Println(b.Addr())
	}
}
