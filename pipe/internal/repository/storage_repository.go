package repository

import (
	"context"
	"fmt"
	"io"
	"os"

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

func (s *StorageRepository) DownloadTempFile(ctx context.Context, fileKey string) (string, func(), error) {
	tracer := otel.Tracer("repository.storage")
	ctx, span := tracer.Start(ctx, "StorageRepository.DownloadTempFile")
	defer span.End()

	obj, err := s.client.GetObject(ctx, s.bucketName, fileKey, minio.GetObjectOptions{})
	if err != nil {
		span.RecordError(err)
		return "", nil, fmt.Errorf("failed to fetch object %s from bucket %s: %w", fileKey, s.bucketName, err)
	}
	defer obj.Close()

	tmpFile, err := os.CreateTemp("", "pdf-stream-*.pdf")
	if err != nil {
		span.RecordError(err)
		return "", nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	_, err = io.Copy(tmpFile, obj)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		span.RecordError(err)
		return "", nil, fmt.Errorf("failed to stream object into temp file: %w", err)
	}

	tmpFilePath := tmpFile.Name()
	tmpFile.Close()

	cleanup := func() {
		os.Remove(tmpFilePath)
	}

	return tmpFilePath, cleanup, nil
}
