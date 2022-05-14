package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	redis "github.com/go-redis/redis/v8"
	_ "github.com/joho/godotenv/autoload"
	twitter "github.com/vniche/twitter-go"
)

var (
	twitterToken  string
	streamName    string
	redisClient   *redis.Client
	twitterClient *twitter.Client
)

func init() {
	var ok bool
	twitterToken, ok = os.LookupEnv("TWITTER_BEARER_TOKEN")
	if !ok {
		log.Fatalf("TWITTER_BEARER_TOKEN env var is required")
	}

	streamName, ok = os.LookupEnv("STREAM_NAME")
	if !ok {
		streamName = "tweets"
	}

	redisHost, ok := os.LookupEnv("REDIS_HOST")
	if !ok {
		redisHost = "localhost:6379"
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0, // use default DB
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("unable to ping redis server: %v", err)
	}
}

func main() {
	// subscribe for process interruption signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// initializes twitter API client
	var err error
	twitterClient, err = twitter.WithBearerToken(twitterToken)
	if err != nil {
		log.Fatalf("unable to initialize twitter client: %+v\n", err)
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
	channel, err = twitterClient.SearchStream(ctx, params)
	if err != nil {
		log.Fatalf("unable to listen to tweets stream: %+v", err)
	}

	fmt.Printf("listening to stream:\n")

	for {
		select {
		case <-quit:
			cancel()
		case <-ctx.Done():
			fmt.Printf("close stream from channel consumer\n")
			channel.Close()
			return
		case message, ok := <-channel.Receive():
			if message == nil || !ok {
				return
			}

			if len(message.Errors) > 0 {
				log.Fatalf("errors from stream: %+v", message.Errors)
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

	if _, err = redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{
			"data": messageBytes,
		},
	}).Result(); err != nil {
		fmt.Printf("publish failed: %+v\n", err)
		fmt.Printf("trying again\n")
		return publish(ctx, message)
	}

	fmt.Printf("msg published\n")
	return nil
}
