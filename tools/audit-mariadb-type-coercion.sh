#!/usr/bin/env bash
# Back-compat wrapper — audits all engines (see audit-type-coercion.sh).
exec "$(dirname "${BASH_SOURCE[0]}")/audit-type-coercion.sh" "$@"
