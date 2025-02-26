# Build stage using Go 1.22.9 on Debian Bookworm
FROM golang:1.22.9-bookworm AS gobuild
WORKDIR /build-dir

# Copy and download Go dependencies
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy source code
COPY internal internal
COPY cmd cmd
COPY pkg pkg

# Prepare OpenAPI files
RUN mkdir -p /tmp/openapi
COPY api/openapi.json /tmp/openapi/openapi.json
COPY api/openapi.yml /tmp/openapi/openapi.yml

# Install development libraries for cgo
RUN apt-get update && \
    apt-get install -y libsecp256k1-dev libsodium-dev

# Build the Go application
RUN go build -o /tmp/opentonapi github.com/tonkeeper/opentonapi/cmd/api

# Runner stage using Ubuntu 22.04
FROM ubuntu:22.04 AS runner

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y openssl ca-certificates libsecp256k1-0 libsodium23 wget && \
    rm -rf /var/lib/apt/lists/*

# Download and configure libemulator.so
RUN mkdir -p /app/lib
RUN wget -O /app/lib/libemulator.so https://github.com/ton-blockchain/ton/releases/download/v2024.08/libemulator-linux-x86_64.so
RUN chmod +x /app/lib/libemulator.so

# Set library path for dynamic linking
ENV LD_LIBRARY_PATH=/app/lib/

# Copy built application and OpenAPI files
COPY --from=gobuild /tmp/opentonapi /usr/bin/
COPY --from=gobuild /tmp/openapi /app/openapi

# Set working directory and command
WORKDIR /app
CMD ["/usr/bin/opentonapi"]