FROM golang:1.26-bookworm AS build
RUN apt-get update && apt-get install -y gcc g++
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -tags=duckdb_arrow -o /duckflight ./cmd/server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /duckflight /usr/local/bin/
EXPOSE 31337 9090
ENTRYPOINT ["duckflight"]
