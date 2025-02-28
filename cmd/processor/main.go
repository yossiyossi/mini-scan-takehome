package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

var (
	services = []string{"HTTP", "SSH", "DNS"}
)

func main() {
	projectId := flag.String("project", "test-project", "GCP Project ID")
	topicId := flag.String("topic", "scan-topic", "GCP PubSub Topic ID")

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, *projectId)
	if err != nil {
		panic(err)
	}

	topic := client.Topic(*topicId)

	sub, err := client.CreateSubscription(context.Background(), "scan-sub", pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		panic(err)
	}

	for range time.Tick(time.Second) {
		// receive a message from the topic

		err = sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
			log.Printf("Got message: %s", m.Data)
			m.Ack()
		})

		if err != nil && !errors.Is(err, context.Canceled) {
			panic(err)
		}
	}
}
