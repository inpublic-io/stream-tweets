package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	_ "github.com/joho/godotenv/autoload"
	twitter "github.com/vniche/twitter-go"
)

var publisher *pscompat.PublisherClient

func main() {
	projectID, ok := os.LookupEnv("GOOGLE_PROJECT_ID")
	if !ok {
		log.Fatalf("GOOGLE_PROJECT_ID env var is required")
	}

	zone, ok := os.LookupEnv("PUBSUB_ZONE")
	if !ok {
		log.Fatalf("PUBSUB_ZONE env var is required")
	}

	topicID, ok := os.LookupEnv("PUBSUB_TOPIC_ID")
	if !ok {
		log.Fatalf("PUBSUB_TOPIC_ID env var is required")
	}

	bearerToken, ok := os.LookupEnv("TWITTER_BEARER_TOKEN")
	if !ok {
		log.Fatalf("PUBSUB_TOPIC_ID env var is required")
	}

	// subscribe for process interruption signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the publisher client.
	var err error
	publisher, err = pscompat.NewPublisherClient(ctx,
		fmt.Sprintf("projects/%s/locations/%s/topics/%s", projectID, zone, topicID),
	)
	if err != nil {
		log.Fatalf("pscompat.NewPublisherClient error: %v", err)
	}
	defer publisher.Stop()

	// initializes twitter API client
	var client *twitter.Client
	client, err = twitter.WithBearerToken(bearerToken)
	if err != nil {
		log.Fatalf("unable to initialize client: %+v\n", err)
	}

	params := make(map[string][]string)
	params["user.fields"] = []string{
		"username",
		"url",
		"location",
		"public_metrics",
	}
	params["tweet.fields"] = []string{
		"entities",
		"created_at",
	}
	params["expansions"] = []string{
		"author_id",
		"entities.mentions.username",
	}

	var channel *twitter.Channel
	channel, err = client.SearchStream(ctx, params)
	if err != nil {
		log.Fatalf("unable to listen to tweets stream: %+v", err)
	}

	for {
		select {
		case <-quit:
			cancel()
		case <-ctx.Done():
			fmt.Printf("close stream from channel consumer\n")
			channel.Close()
			publisher.Stop()
			return
		case message, ok := <-channel.Receive():
			if message == nil || !ok {
				return
			}

			if len(message.Tweet.Entities.Hashtags) <= 10 {
				publish(ctx, message)
			}
		}
	}
}

func publish(ctx context.Context, message *twitter.SearchStreamResponse) error {
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, err = publisher.Publish(ctx, &pubsub.Message{
		Data: messageBytes,
	}).Get(ctx)
	if err != nil {
		fmt.Printf("publish failed, trying again\n")
		return publish(ctx, message)
	}
	fmt.Printf("msg published\n")
	return nil
}
