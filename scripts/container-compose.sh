#!/usr/bin/env bash
# Shared docker/podman compose helper — source from install.sh and systemd wrappers.
# Prefer podman when both exist (docker may be a podman shim on RHEL/Arch).

DATASYNC_CONTAINER_ENGINE="${DATASYNC_CONTAINER_ENGINE:-}"

_container_compose_try_start_podman() {
  command -v systemctl >/dev/null 2>&1 || return 0
  local uid="${DATASYNC_UID:-$(id -u)}"
  local runtime="${XDG_RUNTIME_DIR:-/run/user/${uid}}"
  export XDG_RUNTIME_DIR="${runtime}"
  export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=${runtime}/bus}"
  systemctl --user start podman.socket >/dev/null 2>&1 || true
}

_container_compose_allow_sudo() {
  # systemd / CI must not invoke sudo (no password, wrong identity).
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
    printf '✖ need podman or docker as %s — run: sudo %s/deploy/systemd/install-systemd.sh\n' \
      "$(id -un 2>/dev/null || echo datalake)" \
      "${DATASYNC_ROOT:-/opt/DataSync}" >&2
    printf '  loginctl enable-linger datalake\n' >&2
    printf '  sudo -u datalake env XDG_RUNTIME_DIR=/run/user/$(id -u datalake) systemctl --user enable --now podman.socket\n' >&2
    return 1
  fi

  case "$DATASYNC_CONTAINER_ENGINE" in
    docker)
      docker compose "$@"
      ;;
    docker_sudo)
      sudo docker compose "$@"
      ;;
    podman)
      if podman compose version >/dev/null 2>&1; then
        podman compose "$@"
      elif command -v docker-compose >/dev/null 2>&1; then
        docker-compose "$@"
      else
        printf '✖ podman compose plugin required (pacman -S podman-compose or podman-docker)\n' >&2
        return 1
      fi
      ;;
    *)
      printf '✖ unknown container engine: %s\n' "$DATASYNC_CONTAINER_ENGINE" >&2
      return 1
      ;;
  esac
}

container_runtime_label() {
  ensure_container_runtime || return 0
  case "$DATASYNC_CONTAINER_ENGINE" in
    podman) printf 'podman' ;;
    docker|docker_sudo) printf 'docker' ;;
    *) printf '%s' "$DATASYNC_CONTAINER_ENGINE" ;;
  esac
}
