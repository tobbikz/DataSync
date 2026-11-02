# Contributing to DataSync

Thanks for helping improve on-prem CDC and lake sync.

## Development setup

Requirements: **Docker or Podman + Compose v2**, **Node.js ≥ 20** (for UI), and a C++20 toolchain if you build the daemon natively (or use the Dockerfile).

```bash
git clone https://github.com/tobbikz/DataSync.git
cd DataSync
cp config.json.example config.json   # point at your lab PostgreSQL
docker compose -f docker-compose.dev-engines.yml up -d   # optional engines
./install.sh initial                 # Kafka + daemon + UI
```

UI login (local only): see `ui/.env.example`. Never commit real passwords or `config.json`.

### Native daemon

```bash
cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build cpp/build --target DataSync -j"$(nproc)"
ctest --test-dir cpp/build --output-on-failure
```

Or: `docker build --target test -t datasync-tests:local .`

### UI

```bash
cd ui
cp .env.example .env.local
npm install
npm run dev
```

## Project map

| Path | Role |
|------|------|
| `cpp/src`, `cpp/include` | CDC daemon, capture, apply, catalog |
| `cpp/tests` | C++ unit tests |
| `ui/` | Next.js ops console |
| `tools/` | Lab compose helpers + smoke scripts |
| `docker-compose.yml` | Kafka + daemon + UI |
| `docker-compose.dev-engines.yml` | Lab MariaDB / MSSQL / Mongo / PG |
| `install.sh` | Host lifecycle |
| `config.json.example` | Safe config template |

## Checks before a PR

```bash
docker build --target test -t datasync-tests:local .
cd ui && npm run lint
# Optional lab: ./tools/run-smokes-only.sh  (needs engines + config)
```

Prefer small, focused PRs. Conventional commits (`feat`, `fix`, `docs`, `chore`, …).

## Code style

- Match surrounding C++20 / TypeScript / React patterns.
- User-facing UI copy is **English**.
- Do not add secrets, personal lab configs, absolute machine paths, or customer schema names.
- Keep lab fixtures generic (`lab_src`, `demo_*`); never commit production one-offs.

## Security issues

Do **not** open a public issue for vulnerabilities. See [`SECURITY.md`](SECURITY.md).

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
