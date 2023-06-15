package sqs_test

import (
	"context"
	"testing"

	"github.com/maragudk/is"

	"github.com/maragudk/aws/sqs"
	"github.com/maragudk/aws/sqstest"
)

func TestQueue(t *testing.T) {
	sqstest.SkipIfShort(t)

	t.Run("sends a message to the queue, receives it, and deletes it", func(t *testing.T) {
		q := sqstest.CreateQueue(t)

		err := q.Send(context.Background(), sqs.Message{
			"foo": "bar",
		})
		is.NotError(t, err)

		m, receiptID, err := q.Receive(context.Background())
		is.NotError(t, err)
		is.NotNil(t, m)
		is.Equal(t, "bar", (*m)["foo"])
		is.True(t, len(receiptID) > 0)

		err = q.Delete(context.Background(), receiptID)
		is.NotError(t, err)

		m, _, err = q.Receive(context.Background())
		is.NotError(t, err)
		is.Nil(t, m)
	})

	t.Run("receive does not return an error if the context is already cancelled", func(t *testing.T) {
		queue := sqstest.CreateQueue(t)

		// Send first, to get the queue URL when the context is not cancelled
		err := queue.Send(context.Background(), sqs.Message{})
		is.NotError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		m, _, err := queue.Receive(ctx)
		is.NotError(t, err)
		is.Nil(t, m)
	})
}