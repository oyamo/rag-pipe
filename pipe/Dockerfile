FROM debian:bookworm-slim AS poppler-builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends poppler-utils ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /deps/usr/bin /deps/etc /deps/usr/share && \
    cp /usr/bin/pdftotext /deps/usr/bin/ && \
    for lib in $(ldd /usr/bin/pdftotext | grep -o '/lib/[^ ]*' ; ldd /usr/bin/pdftotext | grep -o '/usr/lib/[^ ]*'); do \
        mkdir -p /deps$(dirname "$lib") && cp -L "$lib" /deps"$lib"; \
    done && \
    if [ -d /etc/fonts ]; then cp -r /etc/fonts /deps/etc/; fi && \
    if [ -d /usr/share/poppler ]; then cp -r /usr/share/poppler /deps/usr/share/; fi && \
    if [ -d /usr/share/fonts ]; then cp -r /usr/share/fonts /deps/usr/share/; fi

FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /app/pipe ./cmd/server

FROM gcr.io/distroless/base-debian12:nonroot

WORKDIR /app

COPY --from=poppler-builder /deps/ /
COPY --from=builder /app/pipe /app/pipe

USER 65532:65532

EXPOSE 8082

ENTRYPOINT ["/app/pipe"]
