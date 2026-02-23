ARG REDIS_VERSION=7.2-bookworm
FROM redis:${REDIS_VERSION}

ARG REDIS_CELL_VERSION=v0.5.0
ARG REDIS_CELL_ASSET=redis-cell-v0.5.0-aarch64-unknown-linux-gnu.tar.gz

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl tar; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    mkdir -p /usr/lib/redis/modules; \
    curl -fsSL -o /tmp/redis-cell.tar.gz \
      "https://github.com/brandur/redis-cell/releases/download/${REDIS_CELL_VERSION}/${REDIS_CELL_ASSET}"; \
    tar -C /tmp -xzf /tmp/redis-cell.tar.gz; \
    cp /tmp/libredis_cell.so /usr/lib/redis/modules/libredis_cell.so; \
    rm -rf /tmp/redis-cell.tar.gz /tmp/libredis_cell.so
