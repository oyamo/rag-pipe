.PHONY: restart restart-clean pull build help

restart:
	docker compose down
	docker rmi ghcr.io/oyamo/ingestion:v0.1 ghcr.io/oyamo/inference:v0.1 ghcr.io/oyamo/pipe:v0.1 --force || true
	docker compose pull
	docker compose up -d
	docker compose ps

restart-clean:
	docker compose down -v --remove-orphans
	docker rmi ghcr.io/oyamo/ingestion:v0.1 ghcr.io/oyamo/inference:v0.1 ghcr.io/oyamo/pipe:v0.1 --force || true
	docker compose pull
	docker compose up -d
	docker compose ps

pull:
	docker compose pull

build:
	docker compose up -d --build

help:
	@echo "Available make targets:"
	@echo "  make restart       - Stop containers, delete old images, pull fresh GHCR images, and start"
	@echo "  make restart-clean - Stop containers & volumes, delete old images, pull fresh GHCR images, and start"
	@echo "  make pull          - Pull latest GHCR images"
	@echo "  make build         - Build local images and start"
