# Go-live checklist (first production / pilot)

Use before the first shared or production install. Pin a release tag (`v1.1.1+`).

UI (default): http://127.0.0.1:3000 · Kafka: `127.0.0.1:9092`.

## 0. Ship the bits

- [ ] Tag and push release (`git push origin v1.1.1`) when you cut one
- [ ] Customer / host can clone this repo and run `./install.sh initial` (or load prebuilt images if you publish them)
- [ ] PostgreSQL reachable for **datasync** (catalog) and **DataLake** (sink)

## 1. Access & secrets

- [ ] `config.json` created from `config.json.example` (mode `600`); never committed
- [ ] Strong passwords for catalog + lake PG users (not lab `DataSync_Dev1!`)
- [ ] `DATASYNC_UI_PASSWORD` and `DATASYNC_UI_SECRET` set (not defaults)
- [ ] UI / Kafka / PG not on the public internet without TLS + ACL (prefer loopback + VPN / reverse proxy)
- [ ] Source CDC accounts are least-privilege and stored in `cdc_catalog.connections`

## 2. First 15 minutes (ops)

1. `./install.sh initial` (or `start`) — Kafka + daemon + UI up
2. Open UI → log in → **Connections** → add / test a source
3. Run **discover** (UI or `DataSync discover`)
4. Onboard tables → **full-load** one small table end-to-end
5. Confirm rows in the lake + catalog health / lag look sane
6. Leave the **daemon** running; watch apply / quarantine / logs

## 3. Source prep (per engine)

| Engine | Prep |
|--------|------|
| MariaDB / MySQL | Binlog `ROW` + `FULL` image; CDC user with REPLICATION privileges |
| SQL Server | CDC or change-tracking enabled on the DB/tables you onboard |
| MongoDB | Replica set; change-stream capable user |

- [ ] Preflight / `test-connection` green before full-load
- [ ] Quarantine recovery known: `DataSync unquarantine` (see [`SECURITY.md`](SECURITY.md) / operator notes)

## 4. Durability & ops

- [ ] Disk watch on `./kafka-data` (retention tuned in `docker-compose.yml`)
- [ ] Catalog + lake backups scheduled (PG dump / your platform standard)
- [ ] Optional: enable [`DataSync.service`](DataSync.service) for reboot persistence
- [ ] Who may run `install.sh start|stop` and rebuild images is documented

## 5. Smoke after cutover

```bash
docker compose run --rm datasync test-connection
docker compose run --rm datasync discover
# UI: dashboard → connections → catalog → logs
```

- [ ] Capture advancing; apply lag acceptable
- [ ] No unexpected quarantine growth
- [ ] DDL sync path understood for schema changes

## Related

- [`INSTALL.md`](INSTALL.md) — full install
- [`SECURITY.md`](SECURITY.md) — reporting + hardening
- [`README.md`](README.md) — product overview
