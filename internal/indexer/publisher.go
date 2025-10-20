package indexer

import (
	"context"
	"log"

	"zond-indexer/internal/config"

	"github.com/streadway/amqp"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/zondclient"
)

const BlocksQueueName = "zond_blocks_queue"

func StartPublisher(ctx context.Context, cfg config.Config) error {
	conn, err := amqp.Dial(cfg.RABBITMQ_URL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(BlocksQueueName, true, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Printf(" RabbitMQ queue '%s' declared and ready.", q.Name)

	client, err := zondclient.Dial(cfg.WSEndpoint)
	if err != nil {
		return err
	}
	defer client.Close()
	log.Printf("Connected to Zond Node WebSocket at %s", cfg.WSEndpoint)

	headers := make(chan *types.Header)
	sub, err := client.SubscribeNewHead(ctx, headers)
	if err != nil {
		return err
	}

	log.Println("🎧 Subscribed to new blocks... Waiting for events.")

	for {
		select {
		case <-ctx.Done():
			log.Println("Publisher is shutting down...")
			sub.Unsubscribe()
			return nil
		case err := <-sub.Err():
			return err
		case header := <-headers:
			blockNumber := header.Number.String()
			log.Printf("📦 New block detected: #%s. Publishing to queue...", blockNumber)
			err = ch.Publish("", q.Name, false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(blockNumber),
			})
			if err != nil {
				log.Printf("Failed to publish block #%s: %v", blockNumber, err)
			}
		}
	}
}
