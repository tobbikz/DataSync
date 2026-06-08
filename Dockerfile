# DataSync — Arch Linux multi-stage image (pacman — same deps as host Arch install)
# Build: podman compose build --network=host datasync
# Install: ./install.sh

FROM docker.io/archlinux/archlinux:latest AS builder

RUN pacman-key --init \
 && pacman-key --populate archlinux \
 && pacman -Sy archlinux-keyring --noconfirm \
 && pacman -Syu --noconfirm \
 && pacman -S --noconfirm --needed \
    base-devel \
    ca-certificates \
    cmake \
    git \
    mariadb-libs \
    libmongoc-1.0 \
    postgresql-libs \
    openssl \
    nlohmann-json \
    pkgconf \
    freetds \
    zlib \
    librdkafka \
 && pacman -Scc --noconfirm \
 && mkdir -p /usr/include/mysql \
 && ln -sf ../mariadb/mysql.h /usr/include/mysql/mysql.h

WORKDIR /src
COPY cpp/ cpp/

RUN mkdir -p cpp/deps/rdkafka/lib cpp/deps/rdkafka/include \
 && cp -a /usr/lib/librdkafka*.so* cpp/deps/rdkafka/lib/ \
 && cp -a /usr/include/librdkafka cpp/deps/rdkafka/include/

RUN cmake -S cpp -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --target DataSync -j"$(nproc)"

# -----------------------------------------------------------------------------

FROM docker.io/archlinux/archlinux:latest AS runtime

RUN pacman-key --init \
 && pacman-key --populate archlinux \
 && pacman -Sy archlinux-keyring --noconfirm \
 && pacman -Syu --noconfirm \
 && pacman -S --noconfirm --needed \
    ca-certificates \
    freetds \
    mariadb-libs \
    libmongoc-1.0 \
    postgresql-libs \
    postgresql \
    python \
    librdkafka \
 && pacman -Scc --noconfirm

COPY --from=builder /usr/lib/librdkafka*.so* /usr/lib/
COPY --from=builder /src/build/DataSync /usr/local/bin/DataSync
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY docker/catalog_bootstrap.py /app/docker/catalog_bootstrap.py
COPY docker/verify_sources.py /app/docker/verify_sources.py
COPY sql/ /app/sql/

RUN chmod +x /usr/local/bin/entrypoint.sh

ENV DATASYNC_ROOT=/app
ENV DATASYNC_CONFIG=/app/config.json
ENV DATASYNC_BIN=/usr/local/bin/DataSync
ENV PATH="/usr/local/bin:${PATH}"

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["daemon"]
