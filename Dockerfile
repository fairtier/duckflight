# Minor version on purpose, not a full patch pin: this repo has no Dependabot
# and the image is only rebuilt on a release tag, so `1.27` resolves to the
# newest 1.27.x at build time rather than to whatever was current when someone
# last edited this line. Move it with the `go` directive in go.mod, and keep
# the ext-download stage below on the same tag.
FROM golang:1.27-trixie AS build
RUN apt-get update && \
    apt-get install -y gcc g++ && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 \
  go build -tags=duckdb_arrow -o /duckflight ./cmd/server

# Download pre-built extensions from the DuckDB extension repositories.
# The lists are build args so a downstream image can bake a different set
# (e.g. community extensions) without forking this file; the defaults keep
# this repo's published image unchanged.
FROM golang:1.27-trixie AS ext-download
ARG TARGETARCH
# Space-separated names, fetched from extensions.duckdb.org. iceberg + runtime
# dependencies not statically linked: json, parquet, icu are already built into
# the duckdb-go binary; avro is a transitive dependency of iceberg.
ARG CORE_EXTENSIONS="iceberg avro httpfs"
# Space-separated names, fetched from community-extensions.duckdb.org.
ARG COMMUNITY_EXTENSIONS=""
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download github.com/duckdb/duckdb-go/v2
RUN DUCKDB_VERSION=$(grep -m1 '^DUCKDB_VERSION=' \
      "$(go env GOMODCACHE)"/github.com/duckdb/duckdb-go/v2@*/Makefile \
      | cut -d= -f2) && \
    DUCKDB_PLATFORM="linux_${TARGETARCH}" && \
    EXT_DIR="/extensions/${DUCKDB_VERSION}/${DUCKDB_PLATFORM}" && \
    mkdir -p "${EXT_DIR}" && \
    for ext in ${CORE_EXTENSIONS}; do \
      curl -fsSL "https://extensions.duckdb.org/${DUCKDB_VERSION}/${DUCKDB_PLATFORM}/${ext}.duckdb_extension.gz" \
        | gunzip > "${EXT_DIR}/${ext}.duckdb_extension"; \
    done && \
    for ext in ${COMMUNITY_EXTENSIONS}; do \
      curl -fsSL "https://community-extensions.duckdb.org/${DUCKDB_VERSION}/${DUCKDB_PLATFORM}/${ext}.duckdb_extension.gz" \
        | gunzip > "${EXT_DIR}/${ext}.duckdb_extension"; \
    done

# Standalone extensions-only image (build with `--target extensions`). Meant
# to run as an initContainer that copies /extensions into an emptyDir mounted
# over the server's EXTENSION_DIR — per-deployment extension sets without
# per-deployment server images. The extension files are tied to the DuckDB
# version inside the duckflight binary, so build this target from the same
# git ref as the server image it will be mounted into.
FROM busybox:stable AS extensions
COPY --from=ext-download /extensions/ /extensions/

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
