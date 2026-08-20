# Multi-stage build for ultra-low memory & fast startup on Railway

# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install git and build tools if needed
RUN apk add --no-cache git

# Copy dependency definitions
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o app .

# Production stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy compiled binary
COPY --from=builder /app/app .

# Railway 0.5GB RAM optimizations & minimal logging
ENV GOMEMLIMIT=400MiB
ENV GOGC=100
ENV VERBOSE_LOGS=false

# Expose Railway port
EXPOSE 8080

# Run application
CMD ["./app"]
