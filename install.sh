#!/usr/bin/env bash
# Host only:
#   ./install.sh initial — first install: config, discover (+ build + stack)
#   ./install.sh start   — pull/restart workflow: rebuild images + recreate stack
#   ./install.sh stop    — stop Kafka + daemon + UI
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

DATASYNC_CONTAINER_ENGINE="${DATASYNC_CONTAINER_ENGINE:-}"

warn() { printf '✖ %s\n' "$*" >&2; }

_container_compose_try_start_podman() {
  command -v systemctl >/dev/null 2>&1 || return 0
  local uid="${DATASYNC_UID:-$(id -u)}"
  local runtime="${XDG_RUNTIME_DIR:-/run/user/${uid}}"
  export XDG_RUNTIME_DIR="${runtime}"
  export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=${runtime}/bus}"
  systemctl --user start podman.socket >/dev/null 2>&1 || true
}

_container_compose_allow_sudo() {
  [[ -z "${INVOCATION_ID:-}" ]] || return 1
  [[ -t 0 ]] || return 1
  command -v sudo >/dev/null 2>&1 || return 1
  sudo -n true 2>/dev/null
}

_container_compose_podman_sock() {
  if [[ -n "${XDG_RUNTIME_DIR:-}" ]]; then
    printf '%s/podman/podman.sock' "$XDG_RUNTIME_DIR"
  else
    printf '/run/user/%s/podman/podman.sock' "$(id -u)"
  fi
}

ensure_container_runtime() {
  [[ -n "$DATASYNC_CONTAINER_ENGINE" ]] && return 0

  _container_compose_try_start_podman

  if [[ -n "${DOCKER_HOST:-}" ]] && command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    DATASYNC_CONTAINER_ENGINE=podman
    export DATASYNC_CONTAINER_ENGINE
    return 0
  fi

  if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    DATASYNC_CONTAINER_ENGINE=podman
    export DATASYNC_CONTAINER_ENGINE
    return 0
  fi

  local sock
  sock="$(_container_compose_podman_sock)"
  if [[ -S "$sock" ]] && command -v podman >/dev/null 2>&1; then
    export DOCKER_HOST="unix://${sock}"
    if podman info >/dev/null 2>&1; then
      DATASYNC_CONTAINER_ENGINE=podman
      export DATASYNC_CONTAINER_ENGINE
      return 0
    fi
  fi

  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    DATASYNC_CONTAINER_ENGINE=docker
    export DATASYNC_CONTAINER_ENGINE
    return 0
  fi

  if _container_compose_allow_sudo && sudo docker info >/dev/null 2>&1; then
    DATASYNC_CONTAINER_ENGINE=docker_sudo
    export DATASYNC_CONTAINER_ENGINE
    return 0
  fi

  return 1
}

docker_compose() {
  if ! ensure_container_runtime; then
    warn "need podman or docker as $(id -un 2>/dev/null || echo datalake)"
    printf '  loginctl enable-linger datalake\n' >&2
    return 1
  fi

  case "$DATASYNC_CONTAINER_ENGINE" in
    docker) docker compose "$@" ;;
    docker_sudo) sudo docker compose "$@" ;;
    podman)
      if podman compose version >/dev/null 2>&1; then
        podman compose "$@"
      elif command -v docker-compose >/dev/null 2>&1; then
        docker-compose "$@"
      else
        warn "podman compose plugin required"
        return 1
      fi
      ;;
    *)
      warn "unknown container engine: $DATASYNC_CONTAINER_ENGINE"
      return 1
      ;;
  esac
}

kafka_tcp_ok() {
  local bootstrap="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
  local host="${bootstrap%%:*}" port="${bootstrap##*:}"
  [[ -n "$host" && -n "$port" ]] || return 1
  (echo >/dev/tcp/"$host"/"$port") 2>/dev/null
}

ensure_kafka_data_dir() {
  local dir="${DATASYNC_KAFKA_DATA:-$ROOT/kafka-data}"
  local created=0

  [[ -d "$dir" ]] || created=1

  if ! mkdir -p "$dir" 2>/dev/null; then
    if command -v sudo >/dev/null 2>&1 && sudo mkdir -p "$dir" 2>/dev/null; then
      :
    else
      warn "could not create ${dir}"
      return 1
    fi
  fi

  if [[ "$created" == "1" ]]; then
    printf '✔ created Kafka data dir: %s\n' "$dir"
  fi

  if ! chmod 777 "$dir" 2>/dev/null; then
    if command -v sudo >/dev/null 2>&1 && sudo chmod 777 "$dir" 2>/dev/null; then
      :
    else
      warn "could not chmod 777 ${dir}"
      return 1
    fi
  fi
}

wait_kafka_compose() {
  local i state
  for i in $(seq 1 120); do
    if kafka_tcp_ok; then
      return 0
    fi
    state="$(docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true)"
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      docker_compose logs kafka --tail 25 >&2 || true
      return 1
    fi
    sleep 2
  done
  warn "Kafka timeout on ${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
  docker_compose logs kafka --tail 25 >&2 || true
  return 1
}

wait_datasync_stopped() {
  local i state
  for i in $(seq 1 130); do
    state="$(docker_compose ps datasync --format '{{.State}}' 2>/dev/null | head -1 || true)"
    if [[ -z "$state" || "$state" == "exited" || "$state" == "dead" ]]; then
      return 0
    fi
    if [[ "$state" != *running* && "$state" != *restarting* ]]; then
      return 0
    fi
    sleep 1
  done
  warn "datasync container did not stop within 130s (grace period is 120s)"
  return 1
}

run_host_discover() {
  if ! kafka_tcp_ok; then
    warn "Kafka not ready — skipping discover"
    return 1
  fi
  docker_compose run --rm --no-deps \
    datasync discover || return 1
}

ensure_config() {
  if [[ -f "$ROOT/config.json" ]]; then
    return 0
  fi
  if [[ -f "$ROOT/config.json.example" ]]; then
    cp "$ROOT/config.json.example" "$ROOT/config.json"
    printf '✔ created config.json from example — edit credentials before prod\n'
    return 0
  fi
  warn "missing config.json (copy config.json.example and edit, or run from repo root)"
  return 1
}

export_compose_ui_env() {
  export DATASYNC_UID="$(id -u)"
  export DATASYNC_GID="$(id -g)"

  case "$DATASYNC_CONTAINER_ENGINE" in
    podman)
      export DATASYNC_CONTAINER_CMD=podman
      export DATASYNC_CONTAINER_BIN="$(command -v podman)"
      export DATASYNC_DOCKER_SOCKET="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock"
      if [[ -z "${DOCKER_HOST:-}" && -S "$DATASYNC_DOCKER_SOCKET" ]]; then
        export DOCKER_HOST="unix://${DATASYNC_DOCKER_SOCKET}"
      fi
      ;;
    docker|docker_sudo)
      export DATASYNC_CONTAINER_CMD=docker
      export DATASYNC_CONTAINER_BIN="$(command -v docker)"
      export DATASYNC_DOCKER_SOCKET=/var/run/docker.sock
      ;;
    *)
      warn "unknown container engine for UI: ${DATASYNC_CONTAINER_ENGINE:-unset}"
      return 1
      ;;
  esac

  if [[ -z "${DATASYNC_CONTAINER_BIN:-}" || ! -x "$DATASYNC_CONTAINER_BIN" ]]; then
    warn "container CLI not found — UI ops actions will be unavailable"
    return 1
  fi
}

host_build_stack() {
  ensure_container_runtime || exit 1
  cd "$ROOT"
  export_compose_ui_env || true
  docker_compose build datasync ui
}

host_stack_initial() {
  ensure_container_runtime || exit 1
  cd "$ROOT"
  ensure_config || exit 1
  export DATASYNC_KAFKA_DATA="${DATASYNC_KAFKA_DATA:-$ROOT/kafka-data}"
  ensure_kafka_data_dir
  export_compose_ui_env || true
  docker_compose stop ui datasync kafka 2>/dev/null || true
  wait_datasync_stopped || true
  host_build_stack
  docker_compose up -d kafka
  wait_kafka_compose || exit 1
  docker_compose up -d --force-recreate --remove-orphans datasync ui
  run_host_discover || exit 1
  docker_compose ps
  printf '✔ initial setup complete — UI http://127.0.0.1:3000\n'
}

host_stack_start() {
  ensure_container_runtime || exit 1
  cd "$ROOT"
  [[ -f "$ROOT/config.json" ]] || {
    warn "missing config.json — run ./install.sh initial first"
    exit 1
  }
  export DATASYNC_KAFKA_DATA="${DATASYNC_KAFKA_DATA:-$ROOT/kafka-data}"
  ensure_kafka_data_dir
  export_compose_ui_env || true
  docker_compose stop ui datasync 2>/dev/null || true
  wait_datasync_stopped || true
  host_build_stack
  docker_compose up -d kafka
  wait_kafka_compose || exit 1
  docker_compose up -d --force-recreate --remove-orphans datasync ui
  docker_compose ps
  printf '✔ stack up (rebuilt) — UI http://127.0.0.1:3000\n'
}

host_stack_stop() {
  ensure_container_runtime || exit 0
  cd "$ROOT"
  export_compose_ui_env || true
  docker_compose stop ui datasync kafka 2>/dev/null || true
  wait_datasync_stopped || true
}

case "${1:-}" in
  initial)
    host_stack_initial
    ;;
  start)
    host_stack_start
    ;;
  stop)
    host_stack_stop
    ;;
  *)
    warn "usage: ./install.sh initial|start|stop"
    exit 2
    ;;
esac
