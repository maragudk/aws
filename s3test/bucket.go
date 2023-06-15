package s3test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/maragudk/env"

	"github.com/maragudk/aws/s3"
)

const (
	defaultBucket = "testbucket"
)

// CreateBucket for testing.
func CreateBucket(t *testing.T) *s3.Bucket {
	env.MustLoad("../.env-test")

	b := s3.NewBucket(s3.NewBucketOptions{
		Config:    getAWSConfig(t),
		Name:      defaultBucket,
		PathStyle: true,
	})

	cleanupBucket(t, b.Client, defaultBucket)
	_, err := b.Client.CreateBucket(context.Background(), &awss3.CreateBucketInput{Bucket: aws.String(defaultBucket)})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		cleanupBucket(t, b.Client, defaultBucket)
	})

	return b
}

func cleanupBucket(t *testing.T, client *awss3.Client, bucket string) {
	listObjectsOutput, err := client.ListObjects(context.Background(), &awss3.ListObjectsInput{Bucket: &bucket})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return
		}
		t.Fatal(err)
	}

	for _, o := range listObjectsOutput.Contents {
		_, err := client.DeleteObject(context.Background(), &awss3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    o.Key,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if _, err := client.DeleteBucket(context.Background(), &awss3.DeleteBucketInput{Bucket: &bucket}); err != nil {
		t.Fatal(err)
	}
}

// SkipIfShort skips t if the "-short" flag is passed to "go test".
func SkipIfShort(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
}

func getAWSConfig(t *testing.T) aws.Config {
	awsConfig, err := config.LoadDefaultConfig(context.Background(),
		config.WithEndpointResolverWithOptions(createAWSEndpointResolver(t)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return awsConfig
}

func createAWSEndpointResolver(t *testing.T) aws.EndpointResolverWithOptionsFunc {
	s3EndpointURL := env.GetStringOrDefault("S3_ENDPOINT_URL", "")
	if s3EndpointURL == "" {
		t.Fatal("s3 endpoint URL must be set in testing with env var S3_ENDPOINT_URL")
	}

	return func(service, region string, options ...any) (aws.Endpoint, error) {
		switch service {
		case awss3.ServiceID:
			return aws.Endpoint{
				URL: s3EndpointURL,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	}
}
