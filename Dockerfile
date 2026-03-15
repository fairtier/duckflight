FROM golang:1.26-trixie AS duckdb-build
RUN apt-get update && apt-get install -y cmake ninja-build git python3 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download github.com/duckdb/duckdb-go/v2
# Extract DuckDB version from the duckdb-go Makefile in the module cache.
RUN DUCKDB_VERSION=$(grep -m1 '^DUCKDB_VERSION=' \
      "$(go env GOMODCACHE)"/github.com/duckdb/duckdb-go/v2@*/Makefile \
      | cut -d= -f2) && \
    git clone --depth 1 --branch "${DUCKDB_VERSION}" https://github.com/duckdb/duckdb.git /duckdb
WORKDIR /duckdb
RUN make bundle-library \
  EXTENSION_CONFIGS='.github/config/extensions/iceberg.cmake' \
  BUILD_EXTENSIONS='icu;json;parquet;autocomplete'

FROM golang:1.26-trixie AS build
RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=duckdb-build /duckdb/build/release/libduckdb_bundle.a /usr/local/lib/
RUN CGO_ENABLED=1 \
  CGO_LDFLAGS="-L/usr/local/lib -lduckdb_bundle -lstdc++ -lm -ldl" \
  go build -tags="duckdb_arrow duckdb_use_static_lib" -o /duckflight ./cmd/server

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* && \
    useradd --no-create-home --shell /usr/sbin/nologin duckflight
COPY --from=build /duckflight /usr/local/bin/
USER duckflight
EXPOSE 31337 9090
ENTRYPOINT ["duckflight"]
