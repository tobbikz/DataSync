# DataSync — multi-stage image (C++ CDC daemon + CLI)
# Host: ./install.sh initial | start | stop  |  Image build is part of start/initial

FROM debian:bookworm-slim AS builder

ARG RDKAFKA_VERSION=v2.6.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    git \
    libmariadb-dev \
    libmongoc-dev \
    libpq-dev \
    libssl-dev \
    nlohmann-json3-dev \
    pkg-config \
    python3 \
    freetds-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /usr/include/mysql \
    && ln -sf ../mariadb/mysql.h /usr/include/mysql/mysql.h

RUN git clone --depth 1 --branch "${RDKAFKA_VERSION}" \
      https://github.com/confluentinc/librdkafka.git /tmp/librdkafka \
    && cd /tmp/librdkafka \
    && ./configure --prefix=/opt/rdkafka --disable-sasl --disable-ssl \
    && make -j"$(nproc)" \
    && make install

WORKDIR /src
COPY cpp/ cpp/

# Vendored librdkafka lives under cpp/build/deps (single out-of-tree build dir)
RUN mkdir -p cpp/build/deps && cp -a /opt/rdkafka cpp/build/deps/rdkafka

ARG BUILD_TESTING=OFF
ARG PARALLEL_JOBS=
RUN cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING="${BUILD_TESTING}" \
    && cmake --build cpp/build --target DataSync -j"${PARALLEL_JOBS:-$(nproc)}"

# Unit-test stage (test binaries only — no DataSync / librdkafka compile):
#   docker build --target test -t datasync-tests:local --build-arg PARALLEL_JOBS=2 .
FROM debian:bookworm-slim AS test
ARG PARALLEL_JOBS=2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    libmariadb-dev \
    libmongoc-dev \
    libpq-dev \
    libssl-dev \
    nlohmann-json3-dev \
    pkg-config \
    freetds-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /usr/include/mysql \
    && ln -sf ../mariadb/mysql.h /usr/include/mysql/mysql.h
WORKDIR /src
COPY cpp/ cpp/
RUN cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON \
    && cmake --build cpp/build -j"${PARALLEL_JOBS}" --target \
         datasync_boolean_tests \
         datasync_envelope_tests \
         datasync_tx_id_tests \
         datasync_gap_kind_tests \
         datasync_coercion_sensitive_tests \
         datasync_full_load_slice_tests \
         datasync_quarantine_hold_tests \
         datasync_column_policy_tests \
         datasync_snapshot_signal_tests \
    && ctest --test-dir cpp/build --output-on-failure --timeout 120

# -----------------------------------------------------------------------------

FROM debian:bookworm-slim AS runtime

# mariadb-client = CLI only (mariadb-binlog reads binlog from host :3306). No MariaDB server in this image.
# bookworm default client 3.5 cannot parse binlog protocol from MariaDB 12.x — use MariaDB repo 12.3.
ARG MARIADB_REPO_VERSION=12.3
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    && curl -LsS https://r.mariadb.com/downloads/mariadb_repo_setup \
      | bash -s -- --mariadb-server-version="${MARIADB_REPO_VERSION}" --os-type=debian --os-version=bookworm \
    && apt-get install -y --no-install-recommends \
    freetds-common \
    libmongoc-1.0-0 \
    libmariadb3 \
    mariadb-client \
    libpq5 \
    libsybdb5 \
    postgresql-client \
    python3 \
    && apt-get purge -y curl gnupg \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/rdkafka/lib /opt/rdkafka/lib
COPY --from=builder /src/cpp/build/DataSync /usr/local/bin/DataSync
COPY docker-entrypoint.sh /app/

RUN chmod +x /app/docker-entrypoint.sh

ENV LD_LIBRARY_PATH=/opt/rdkafka/lib
ENV DATASYNC_ROOT=/app
ENV DATASYNC_CONFIG=/app/config.json
ENV DATASYNC_BIN=/usr/local/bin/DataSync
ENV PATH="/usr/local/bin:${PATH}"

WORKDIR /app

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["daemon"]
