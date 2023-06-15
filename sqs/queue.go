package sqs

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Message = map[string]any

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

// Send a message as JSON.
func (q *Queue) Send(ctx context.Context, m Message) error {
	if err := q.ensureQueueURL(ctx); err != nil {
		return err
	}

	messageAsBytes, err := json.Marshal(m)
	if err != nil {
		return err
	}
	messageAsString := string(messageAsBytes)

	_, err = q.Client.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: &messageAsString,
		QueueUrl:    q.url,
	})

	return err
}

// Receive a message and its receipt ID. Returns nil if no message is available.
func (q *Queue) Receive(ctx context.Context) (*Message, string, error) {
	if err := q.ensureQueueURL(ctx); err != nil {
		return nil, "", err
	}

	output, err := q.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:        q.url,
		WaitTimeSeconds: int32(q.waitTime.Seconds()),
	})
	if err != nil {
		if strings.Contains(err.Error(), "context canceled") {
			return nil, "", nil
		}
		return nil, "", err
	}

	if len(output.Messages) == 0 {
		return nil, "", nil
	}

	var m Message
	if err := json.Unmarshal([]byte(*output.Messages[0].Body), &m); err != nil {
		return nil, "", err
	}

	return &m, *output.Messages[0].ReceiptHandle, nil
}

// Delete a message by receipt ID.
func (q *Queue) Delete(ctx context.Context, receiptID string) error {
	if err := q.ensureQueueURL(ctx); err != nil {
		return err
	}

	_, err := q.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      q.url,
		ReceiptHandle: &receiptID,
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
