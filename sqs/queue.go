package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Message struct {
	Body          Body
	ReceiptHandle string
}

type Body = map[string]string

type Queue struct {
	Client   *sqs.Client
	mutex    sync.Mutex
	name     string
	url      *string
	waitTime time.Duration
}

type NewQueueOptions struct {
	Config   aws.Config
	Name     string
	WaitTime time.Duration
}

func NewQueue(opts NewQueueOptions) *Queue {
	if opts.Name == "" {
		panic("queue name must not be empty")
	}

	if opts.WaitTime < 0 || opts.WaitTime > 20*time.Second {
		panic("queue wait time must be between 0 and 20 seconds, both inclusive")
	}

	return &Queue{
		Client:   sqs.NewFromConfig(opts.Config),
		name:     opts.Name,
		waitTime: opts.WaitTime,
	}
}

// Send a message body as JSON.
func (q *Queue) Send(ctx context.Context, b Body) error {
	if err := q.ensureQueueURL(ctx); err != nil {
		return err
	}

	bodyBytes, err := json.Marshal(b)
	if err != nil {
		return err
	}
	bodyString := string(bodyBytes)

	_, err = q.Client.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: &bodyString,
		QueueUrl:    q.url,
	})

	return err
}

// Receive a Message. Returns nil if no message is available.
func (q *Queue) Receive(ctx context.Context) (*Message, error) {
	if err := q.ensureQueueURL(ctx); err != nil {
		return nil, err
	}

	output, err := q.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:        q.url,
		WaitTimeSeconds: int32(q.waitTime.Seconds()),
	})
	if err != nil {
		if strings.Contains(err.Error(), "context canceled") {
			return nil, nil
		}
		return nil, err
	}

	if len(output.Messages) == 0 {
		return nil, nil
	}

	var m Message
	if err := json.Unmarshal([]byte(*output.Messages[0].Body), &m.Body); err != nil {
		return nil, err
	}

	m.ReceiptHandle = *output.Messages[0].ReceiptHandle

	return &m, nil
}

// Delete a Message.
// Does nothing if the passed Message is nil.
func (q *Queue) Delete(ctx context.Context, m *Message) error {
	if err := q.ensureQueueURL(ctx); err != nil {
		return err
	}

	if m == nil {
		return nil
	}

	_, err := q.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      q.url,
		ReceiptHandle: &m.ReceiptHandle,
	})

	return err
}

// SetTimeout sets the visibility timeout for the Message, overwriting any existing timeout.
// The timeout is set from now.
// Does nothing if the Message is nil.
func (q *Queue) SetTimeout(ctx context.Context, m *Message, timeout time.Duration) error {
	if err := q.ensureQueueURL(ctx); err != nil {
		return err
	}

	if m == nil {
		return nil
	}

	if timeout < 0 || timeout > 12*time.Hour {
		return errors.New("timeout must be between 0 and 12 hours, both inclusive")
	}

	_, err := q.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          q.url,
		ReceiptHandle:     &m.ReceiptHandle,
		VisibilityTimeout: int32(timeout.Seconds()),
	})

	return err
}

// ensureQueueURL under a lock.
func (q *Queue) ensureQueueURL(ctx context.Context) error {
	if q.url != nil {
		return nil
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Check again after the lock, we might have the URL already.
	if q.url != nil {
		return nil
	}

	output, err := q.Client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &q.name,
	})
	if err != nil {
		return err
	}
	q.url = output.QueueUrl

	return nil
}
