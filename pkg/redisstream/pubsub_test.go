package redisstream

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	"github.com/stretchr/testify/require"
)

var (
	client redis.UniversalClient
)

func redisClient() (redis.UniversalClient, error) {
	if client == nil {
		client = redis.NewClient(&redis.Options{
			Addr:         "127.0.0.1:6380",
			DB:           0,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			MinIdleConns: 10,
		})
		err := client.Ping().Err()
		if err != nil {
			return nil, errors.Wrap(err, "redis simple connect fail")
		}
	}
	return client, nil
}

func newPubSub(t *testing.T, marshaler MarshalerUnmarshaler, subConfig *SubscriberConfig) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	rc, err := redisClient()
	require.NoError(t, err)

	publisher, err := NewPublisher(rc, marshaler, logger)
	require.NoError(t, err)

	subscriber, err := NewSubscriber(*subConfig, rc, marshaler, logger)
	require.NoError(t, err)

	return publisher, subscriber
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, shortuuid.New())
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, &DefaultMarshaler{}, &SubscriberConfig{
		Consumer:        shortuuid.New(),
		ConsumerGroup:   consumerGroup,
		DoNotDelMessage: true,
	})
}

func createPubSubWithDel(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroupWithDel(t, shortuuid.New())
}

func createPubSubWithConsumerGroupWithDel(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, &DefaultMarshaler{}, &SubscriberConfig{
		Consumer:        shortuuid.New(),
		ConsumerGroup:   consumerGroup,
		DoNotDelMessage: false,
	})
}

func TestPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:                      true,
		ExactlyOnceDelivery:                 true,
		GuaranteedOrder:                     false,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		//RestartServiceCommand: []string{
		//	`docker`,
		//	`restart`,
		//	`redis-simple`,
		//},
		RequireSingleInstance:            false,
		NewSubscriberReceivesOldMessages: true,
	}

	tests.TestPubSub(t, features, createPubSub, createPubSubWithConsumerGroup)
}

func TestPublishSubscribeWithDel(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:                      false,
		ExactlyOnceDelivery:                 true,
		GuaranteedOrder:                     false,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		//RestartServiceCommand: []string{
		//	`docker`,
		//	`restart`,
		//	`redis-simple`,
		//},
		RequireSingleInstance:            false,
		NewSubscriberReceivesOldMessages: true,
	}

	tests.TestPubSub(t, features, createPubSubWithDel, createPubSubWithConsumerGroupWithDel)
}

func TestSubscriber(t *testing.T) {
	topic := "test-topic1"
	rc, err := redisClient()
	require.NoError(t, err)
	publisher, err := NewPublisher(rc, &DefaultMarshaler{}, watermill.NewStdLogger(false, false))
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		require.NoError(t, publisher.Publish(topic, message.NewMessage(shortuuid.New(), []byte("test"+strconv.Itoa(i)))))
	}
	require.NoError(t, publisher.Close())

	subscriber, err := NewSubscriber(
		SubscriberConfig{
			Consumer:        "consumer1",
			ConsumerGroup:   "test-consumer-group",
			DoNotDelMessage: false,
		},
		rc,
		&DefaultMarshaler{},
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)
	messages, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		msg := <-messages
		if msg == nil {
			t.Fatal("msg nil")
		}
		t.Logf("%v %v %v", msg.UUID, msg.Metadata, string(msg.Payload))
		msg.Ack()
	}

	require.NoError(t, subscriber.Close())
}
