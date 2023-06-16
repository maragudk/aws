package sqs_test

import (
	"context"
	"testing"
	"time"

	"github.com/maragudk/is"

	"github.com/maragudk/aws/sqs"
	"github.com/maragudk/aws/sqstest"
)

func TestQueue(t *testing.T) {
	sqstest.SkipIfShort(t)

	t.Run("sends a message to the queue, receives it, and deletes it", func(t *testing.T) {
		q := sqstest.CreateQueue(t)

		err := q.Send(context.Background(), sqs.Body{
			"foo": "bar",
		})
		is.NotError(t, err)

		m, err := q.Receive(context.Background())
		is.NotError(t, err)
		is.NotNil(t, m)
		is.Equal(t, "bar", m.Body["foo"])
		is.True(t, len(m.ReceiptHandle) > 0)

		err = q.Delete(context.Background(), m)
		is.NotError(t, err)

		m, err = q.Receive(context.Background())
		is.NotError(t, err)
		is.Nil(t, m)
	})

	t.Run("receive does not return an error if the context is already cancelled", func(t *testing.T) {
		q := sqstest.CreateQueue(t)

		// Send first, to get the queue URL when the context is not cancelled
		err := q.Send(context.Background(), sqs.Body{})
		is.NotError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		m, err := q.Receive(ctx)
		is.NotError(t, err)
		is.Nil(t, m)
	})
}

func TestQueue_SetTimeout(t *testing.T) {
	sqstest.SkipIfShort(t)

	t.Run("can set timeout", func(t *testing.T) {
		q := sqstest.CreateQueue(t)

		err := q.Send(context.Background(), sqs.Body{})
		is.NotError(t, err)

		m, err := q.Receive(context.Background())
		is.NotError(t, err)
		is.NotNil(t, m)

		m2, err := q.Receive(context.Background())
		is.NotError(t, err)
		is.Nil(t, m2)

		err = q.SetTimeout(context.Background(), m, 0)
		is.NotError(t, err)

		m2, err = q.Receive(context.Background())
		is.NotError(t, err)
		is.NotNil(t, m2)
	})

	t.Run("errors on timeouts outside [0,12 hours]", func(t *testing.T) {
		q := sqstest.CreateQueue(t)

		err := q.Send(context.Background(), sqs.Body{})
		is.NotError(t, err)

		m, err := q.Receive(context.Background())
		is.NotError(t, err)
		is.NotNil(t, m)

		err = q.SetTimeout(context.Background(), m, -1)
		is.True(t, err != nil)
		is.Equal(t, "timeout must be between 0 and 12 hours, both inclusive", err.Error())

		err = q.SetTimeout(context.Background(), m, 12*time.Hour+1)
		is.True(t, err != nil)
		is.Equal(t, "timeout must be between 0 and 12 hours, both inclusive", err.Error())
	})
}
