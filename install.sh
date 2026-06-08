#!/usr/bin/env bash
# DataSync — Docker install (Kafka + daemon). Idempotent schema bootstrap.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      sed -n '2,8p' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

warn() { printf '✖ %s\n' "$*" >&2; }

# shellcheck source=scripts/container-compose.sh
source "$ROOT/scripts/container-compose.sh"

run_quiet() {
  local label="$1"; shift
  local err
  err="$(mktemp)"
  if "$@" >"$err" 2>&1; then
    rm -f "$err"
    printf '✔ %s\n' "$label"
    return 0
  fi
  printf '✖ %s\n' "$label" >&2
  tail -20 "$err" >&2 || true
  rm -f "$err"
  return 1
}

ensure_container_engine() {
  if ensure_container_runtime; then
    printf '✔ Container runtime (%s)\n' "$(container_runtime_label)"
    return 0
  fi
  warn "podman or docker required — start rootless podman: systemctl --user start podman.socket"
  exit 1
}

ensure_config() {
  if [[ ! -f "$ROOT/config.json" ]]; then
    if [[ -f "$ROOT/config.json.example" ]]; then
      cp "$ROOT/config.json.example" "$ROOT/config.json"
      warn "created config.json from example — edit passwords and re-run"
      exit 1
    fi
    warn "missing config.json"
    exit 1
  fi
}

build_image() {
  local t0=$SECONDS err
  err="$(mktemp)"
  if docker_compose build --network=host --quiet datasync >"$err" 2>&1; then
    rm -f "$err"
    printf '✔ Image datasync built %ss\n' "$((SECONDS - t0))"
    return 0
  fi
  if docker_compose build --network=host datasync >"$err" 2>&1; then
    rm -f "$err"
    printf '✔ Image datasync built %ss\n' "$((SECONDS - t0))"
    return 0
  fi
  if [[ "${NATIVE_BUILD:-0}" == "1" ]] || [[ -f /etc/arch-release ]]; then
    if build_image_native >"$err" 2>&1; then
      rm -f "$err"
      err="$(mktemp)"
      if COMPOSE_FILE="docker-compose.yml:docker-compose.packaged.yml" \
         docker_compose build --network=host datasync >"$err" 2>&1; then
        rm -f "$err"
        printf '✔ DataSync packaged image (native binary) %ss\n' "$((SECONDS - t0))"
        export DATASYNC_NATIVE_BINARY=1
        return 0
      fi
      tail -20 "$err" >&2 || true
      rm -f "$err"
    fi
  fi
  printf '✖ Image datasync build failed\n' >&2
  tail -30 "$err" >&2 || true
  rm -f "$err"
  warn "retry with host DNS: podman compose build --network=host datasync"
  warn "or native: NATIVE_BUILD=1 ./install.sh (pacman deps on Arch host)"
  return 1
}

build_image_native() {
  command -v cmake >/dev/null 2>&1 || return 1
  command -v g++ >/dev/null 2>&1 || return 1
  if command -v pacman >/dev/null 2>&1; then
    pacman -Q mariadb-libs postgresql-libs cmake base-devel >/dev/null 2>&1 || {
      printf 'install deps: sudo pacman -S base-devel cmake mariadb-libs postgresql-libs nlohmann-json freetds libmongoc-1.0 librdkafka\n' >&2
      return 1
    }
  fi
  local build_dir="$ROOT/cpp/build"
  cmake -S "$ROOT/cpp" -B "$build_dir" -DCMAKE_BUILD_TYPE=Release
  cmake --build "$build_dir" --target DataSync -j"$(nproc)"
  [[ -x "$build_dir/DataSync" ]]
}

apply_catalog_schema() {
  [[ -f "$ROOT/sql/backup/cdc_catalog_schema_structure.sql" ]] || { warn "missing catalog schema sql"; exit 1; }
  local err rc
  err="$(mktemp)"
  docker_compose run --rm --no-deps --remove-orphans \
    -e DATASYNC_RUN_MIGRATIONS=1 \
    -e DATASYNC_INSTALL_QUIET=1 \
    -e DATASYNC_HOST_NETWORK=1 \
    -e KAFKA_BOOTSTRAP=localhost:9092 \
    datasync schema-only >"$err" 2>&1
  rc=$?
  grep -vE 'Container datasync-datasync-run|^$' "$err" || true
  rm -f "$err"
  if [[ "$rc" -ne 0 ]]; then
    exit "$rc"
  fi
}

wait_zookeeper() {
  local i h
  for i in $(seq 1 30); do
    h=$(docker_compose ps zookeeper --format '{{.Health}}' 2>/dev/null | head -1 || true)
    if [[ "$h" == "healthy" ]]; then
      return 0
    fi
    sleep 1
  done
  warn "Zookeeper not ready — check: $(container_runtime_label) compose logs zookeeper --tail 30"
  return 1
}

wait_kafka() {
  local i h state
  for i in $(seq 1 60); do
    state=$(docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true)
    h=$(docker_compose ps kafka --format '{{.Health}}' 2>/dev/null | head -1 || true)
    if [[ "$state" == "running" && "$h" == "healthy" ]]; then
      printf '✔ Kafka ready\n'
      return 0
    fi
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      break
    fi
    sleep 2
  done
  warn "Kafka not ready — check: $(container_runtime_label) compose logs kafka --tail 40"
  return 1
}

# Kafka must register in ZK after ZK is up. If Kafka alone restarts while ZK keeps a
# stale /brokers/ids/* ephemeral, startup fails with NodeExists — restart ZK first.
start_zookeeper_kafka() {
  docker_compose up -d --remove-orphans zookeeper
  wait_zookeeper || return 1

  local kstate khealth
  kstate=$(docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true)
  khealth=$(docker_compose ps kafka --format '{{.Health}}' 2>/dev/null | head -1 || true)

  if [[ "$kstate" == "running" && "$khealth" == "healthy" ]]; then
    docker_compose up -d kafka
    wait_kafka
    return $?
  fi

  docker_compose stop kafka 2>/dev/null || true
  docker_compose restart zookeeper
  wait_zookeeper || return 1
  docker_compose up -d --force-recreate kafka
  wait_kafka
}

run_discover() {
  printf '✔ Discover skipped (run manually after onboarding sources)\n'
  return 0
}

post_install_health() {
  local err rc
  err="$(mktemp)"
  docker_compose run --rm --no-deps --remove-orphans \
    -e DATASYNC_INSTALL_QUIET=1 \
    -e DATASYNC_HOST_NETWORK=1 \
    -e KAFKA_BOOTSTRAP=localhost:9092 \
    datasync health-only >"$err" 2>&1
  rc=$?
  grep -vE 'Container datasync-datasync-run|^$' "$err" || true
  rm -f "$err"
  if [[ "$rc" -eq 0 ]]; then
    printf '✔ Post-install health\n'
    return 0
  fi
  warn "post-install health failed"
  return 1
}

install_and_enable_systemd() {
  [[ "${SKIP_SYSTEMD:-0}" == "1" ]] && return 0
  [[ -x "$ROOT/deploy/systemd/install-systemd.sh" ]] || return 0

  run_systemctl() {
    if [[ "${EUID}" -eq 0 ]]; then
      "$@"
    elif command -v sudo >/dev/null 2>&1; then
      sudo "$@"
    else
      return 1
    fi
  }

  local err rc
  err="$(mktemp)"
  if run_systemctl "$ROOT/deploy/systemd/install-systemd.sh" >"$err" 2>&1; then
    rm -f "$err"
  else
    rc=$?
    warn "systemd install failed — run: sudo deploy/systemd/install-systemd.sh"
    if [[ "$rc" -eq 1 ]] && grep -q 'Run as root' "$err" 2>/dev/null; then
      warn "needs root (sudo password when prompted, or run the command above)"
    fi
    tail -8 "$err" >&2 || true
    rm -f "$err"
    return 0
  fi
  printf '✔ systemd units installed\n'

  if run_systemctl systemctl enable --now DataSync >/dev/null 2>&1; then
    printf '✔ systemd DataSync enabled\n'
  else
    warn "systemctl enable DataSync failed (sudo required)"
  fi

  if run_systemctl systemctl enable --now DataSync-reconcile.timer >/dev/null 2>&1; then
    printf '✔ systemd reconcile.timer enabled\n'
  else
    warn "systemctl enable DataSync-reconcile.timer failed"
  fi
}

ensure_container_engine
ensure_config

build_image
apply_catalog_schema

run_quiet "Zookeeper + Kafka starting" start_zookeeper_kafka

run_quiet "DataSync daemon" docker_compose up -d --no-recreate datasync

sleep 2
run_quiet "Post-install health" post_install_health
run_discover
install_and_enable_systemd

printf '✔ Install complete — status: %s compose ps | discover: %s compose run --rm datasync discover\n' \
  "$(container_runtime_label)" "$(container_runtime_label)"
