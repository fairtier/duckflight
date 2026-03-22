FROM golang:1.26-trixie AS build
RUN apt-get update && \
    apt-get install -y gcc g++ && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 \
  go build -tags=duckdb_arrow -o /duckflight ./cmd/server

# Download pre-built iceberg extension from the DuckDB extension repository.
FROM golang:1.26-trixie AS ext-download
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download github.com/duckdb/duckdb-go/v2
RUN DUCKDB_VERSION=$(grep -m1 '^DUCKDB_VERSION=' \
      "$(go env GOMODCACHE)"/github.com/duckdb/duckdb-go/v2@*/Makefile \
      | cut -d= -f2) && \
    DUCKDB_PLATFORM="linux_${TARGETARCH}" && \
    mkdir -p "/extensions/${DUCKDB_VERSION}/${DUCKDB_PLATFORM}" && \
    curl -fsSL "https://extensions.duckdb.org/${DUCKDB_VERSION}/${DUCKDB_PLATFORM}/iceberg.duckdb_extension.gz" \
      | gunzip > "/extensions/${DUCKDB_VERSION}/${DUCKDB_PLATFORM}/iceberg.duckdb_extension"

FROM debian:trixie-slim
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --no-create-home --shell /usr/sbin/nologin duckflight
COPY --from=build /duckflight /usr/local/bin/
COPY --from=ext-download /extensions/ /extensions/
ENV EXTENSION_DIR=/extensions
USER duckflight
EXPOSE 31337 9090
ENTRYPOINT ["duckflight"]
