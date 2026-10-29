# Shared lab helpers for Git Bash on Windows (source from smokes / bootstrap).
# shellcheck shell=bash

host_path() {
  local p="$1"
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$p"
  else
    printf '%s' "$p"
  fi
}

lab_py() {
  if command -v python >/dev/null 2>&1; then
    python "$@"
  else
    python3 "$@"
  fi
}

# Normalize DATASYNC_CONFIG to a docker.exe / Windows-python friendly path.
if [[ -n "${DATASYNC_CONFIG:-}" ]]; then
  DATASYNC_CONFIG="$(host_path "$DATASYNC_CONFIG")"
elif [[ -n "${ROOT:-}" && -f "$ROOT/config.json" ]]; then
  DATASYNC_CONFIG="$(host_path "$ROOT/config.json")"
fi
export DATASYNC_CONFIG
