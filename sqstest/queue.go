package sqstest

import (
	"context"
	"testing"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/maragudk/env"

	"github.com/maragudk/aws/awstest"
	"github.com/maragudk/aws/sqs"
)

// CreateQueue for testing.
func CreateQueue(t *testing.T) *sqs.Queue {
	env.MustLoad("../.env-test")

	name := env.GetStringOrDefault("QUEUE_NAME", "q")
	queue := sqs.NewQueue(sqs.NewQueueOptions{
		Config: awstest.GetAWSConfig(t),
		Name:   name,
	})

	createQueueOutput, err := queue.Client.CreateQueue(context.Background(), &awssqs.CreateQueueInput{
		QueueName: &name,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, err := queue.Client.DeleteQueue(context.Background(), &awssqs.DeleteQueueInput{
			QueueUrl: createQueueOutput.QueueUrl,
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	return queue
}

// SkipIfShort skips the current test if the short flag is set. Used to not run integration tests always.
func SkipIfShort(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
}
