package redisstream

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/renstrom/shortuuid"
)

func BenchmarkSubscriber(b *testing.B) {
	rc, err := redisClient()
	if err != nil {
		b.Fatal(err)
	}
	tests.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		logger := watermill.NopLogger{}

		publisher, err := NewPublisher(rc, &DefaultMarshaler{}, logger)
		if err != nil {
			panic(err)
		}

		subscriber, err := NewSubscriber(
			SubscriberConfig{
				Consumer:        shortuuid.New(),
				ConsumerGroup:   shortuuid.New(),
				DoNotDelMessage: false,
			},
			rc,
			&DefaultMarshaler{},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return publisher, subscriber
	})
}
