package s3

import (
	"context"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Bucket struct {
	Client *s3.Client
	log    *log.Logger
	name   string
}

type NewBucketOptions struct {
	Config    aws.Config
	Log       *log.Logger
	Name      string
	PathStyle bool
}

// NewBucket with the given options.
// If no logger is provided, logs are discarded.
func NewBucket(opts NewBucketOptions) *Bucket {
	if opts.Log == nil {
		opts.Log = log.New(io.Discard, "", 0)
	}

	client := s3.NewFromConfig(opts.Config, func(o *s3.Options) {
		o.UsePathStyle = opts.PathStyle
	})

	return &Bucket{
		Client: client,
		log:    opts.Log,
		name:   opts.Name,
	}
}

// Put an object under key with the given contentType.
func (b *Bucket) Put(ctx context.Context, key, contentType string, body io.Reader) error {
	_, err := b.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &b.name,
		Key:         &key,
		Body:        body,
		ContentType: &contentType,
	})
	return err
}

// Get an object under key.
// If there is nothing there, returns nil and no error.
func (b *Bucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	getObjectOutput, err := b.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &key,
	})
	if getObjectOutput == nil {
		return nil, nil
	}
	return getObjectOutput.Body, err
}

// Delete an object under key.
// Deleting where nothing exists does nothing and returns no error.
func (b *Bucket) Delete(ctx context.Context, key string) error {
	_, err := b.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &b.name,
		Key:    &key,
	})
	return err
}
