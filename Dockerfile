# DataSync — multi-stage image (C++ CDC daemon + CLI)
# Build: docker build -t datasync:local .
# Install: ./install.sh

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

# CMakeLists expects librdkafka under cpp/deps/rdkafka (built in this stage, not from host)
RUN mkdir -p cpp/deps && cp -a /opt/rdkafka cpp/deps/rdkafka

RUN cmake -S cpp -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF \
    && cmake --build build --target DataSync -j"$(nproc)"

# -----------------------------------------------------------------------------

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    freetds-common \
    libmongoc-1.0-0 \
    libmariadb3 \
    mariadb-client \
    libpq5 \
    libsybdb5 \
    postgresql-client \
    python3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/rdkafka/lib /opt/rdkafka/lib
COPY --from=builder /src/build/DataSync /usr/local/bin/DataSync
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY docker/catalog_bootstrap.py /app/docker/catalog_bootstrap.py
COPY docker/verify_sources.py /app/docker/verify_sources.py
COPY sql/ /app/sql/

RUN chmod +x /usr/local/bin/entrypoint.sh

ENV LD_LIBRARY_PATH=/opt/rdkafka/lib
ENV DATASYNC_ROOT=/app
ENV DATASYNC_CONFIG=/app/config.json
ENV DATASYNC_BIN=/usr/local/bin/DataSync
ENV PATH="/usr/local/bin:${PATH}"

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["daemon"]
