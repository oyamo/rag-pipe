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

func (s *StorageRepository) FetchObjectReader(ctx context.Context, fileKey string) (io.ReadCloser, error) {
	_, span := otel.Tracer("repository.storage").Start(ctx, "StorageRepository.FetchObjectReader")
	defer span.End()

	obj, err := s.client.GetObject(ctx, s.bucketName, fileKey, minio.GetObjectOptions{})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to fetch object %s: %w", fileKey, err)
	}

	return obj, nil
}
