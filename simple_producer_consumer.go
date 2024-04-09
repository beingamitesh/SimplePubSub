package main

import (
	"fmt"
	"sync"
	"time"
)

type Message struct {
	Value string
}

type Topic struct {
	Name        string
	PartitionCh chan *Message // Channel for incoming messages
}

type Kafka struct {
	Topics map[string]*Topic
}

func NewKafka() *Kafka {
	return &Kafka{
		Topics: make(map[string]*Topic),
	}
}

func (k *Kafka) CreateTopic(name string) {
	k.Topics[name] = &Topic{
		Name:        name,
		PartitionCh: make(chan *Message),
	}
}

func (k *Kafka) Produce(topicName string, message *Message) {
	topic, ok := k.Topics[topicName]
	if !ok {
		fmt.Println("Topic not found")
		return
	}

	topic.PartitionCh <- message // Send message to partition channel
}

func (k *Kafka) Consume(topicName string, wg *sync.WaitGroup) {
	defer wg.Done()

	topic, ok := k.Topics[topicName]
	if !ok {
		fmt.Println("Topic not found")
		return
	}

	for message := range topic.PartitionCh {
		fmt.Printf("Consumed message from topic %s: %s\n", topicName, message.Value)
	}

	fmt.Printf("Consumer for topic %s finished\n", topicName)
}

func main() {
	kafka := NewKafka()

	kafka.CreateTopic("topic1")

	var wg sync.WaitGroup

	// Simulate producing messages
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			kafka.Produce("topic1", &Message{Value: fmt.Sprintf("Message %d", i)})
			time.Sleep(time.Second)
		}
		// Close the channel once all messages are produced
		close(kafka.Topics["topic1"].PartitionCh)
	}()

	// Simulate consuming messages
	go func() {
		kafka.Consume("topic1", &wg)
	}()

	// Wait for all goroutines to finish
	wg.Wait()
}
