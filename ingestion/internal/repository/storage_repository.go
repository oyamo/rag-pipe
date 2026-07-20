package repository

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel"
)

type StorageRepository struct {
	client     *minio.Client
	bucketName string
}

func NewStorageRepository(endpoint, accessKey, secretKey, bucketName string, useSSL bool) (*StorageRepository, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize minio client: %w", err)
	}

	return &StorageRepository{
		client:     client,
		bucketName: bucketName,
	}, nil
}

func (s *StorageRepository) EnsureBucket(ctx context.Context) error {
	tracer := otel.Tracer("repository.storage")
	ctx, span := tracer.Start(ctx, "StorageRepository.EnsureBucket")
	defer span.End()

	exists, err := s.client.BucketExists(ctx, s.bucketName)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !exists {
		err = s.client.MakeBucket(ctx, s.bucketName, minio.MakeBucketOptions{})
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return nil
}

func (s *StorageRepository) UploadFile(ctx context.Context, objectName string, reader io.Reader, objectSize int64, contentType string) (string, error) {
	tracer := otel.Tracer("repository.storage")
	ctx, span := tracer.Start(ctx, "StorageRepository.UploadFile")
	defer span.End()

	err := s.EnsureBucket(ctx)
	if err != nil {
		span.RecordError(err)
		return "", err
	}

	info, err := s.client.PutObject(
		ctx,
		s.bucketName,
		objectName,
		reader,
		objectSize,
		minio.PutObjectOptions{
			ContentType: contentType,
		},
	)
	if err != nil {
		span.RecordError(err)
		return "", fmt.Errorf("failed to upload object to minio: %w", err)
	}

	return info.Key, nil
}
