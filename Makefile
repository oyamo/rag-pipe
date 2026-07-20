.PHONY: restart pull build help

restart:
	docker compose stop ingestion pipe inference
	docker compose rm -f ingestion pipe inference
	docker rmi ghcr.io/oyamo/ingestion:v0.1 ghcr.io/oyamo/inference:v0.1 ghcr.io/oyamo/pipe:v0.1 --force || true
	docker compose pull ingestion pipe inference
	docker compose up -d ingestion pipe inference
	docker compose ps

pull:
	docker compose pull ingestion pipe inference

build:
	docker compose up -d --build ingestion pipe inference

help:
	@echo "Available make targets:"
	@echo "  make restart - Stop & update app containers (ingestion, pipe, inference) without touching Postgres, MinIO, or NATS infra"
	@echo "  make pull    - Pull latest app images"
	@echo "  make build   - Build local app images and start"
