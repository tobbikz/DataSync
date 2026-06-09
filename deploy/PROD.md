# DataSync — producción (Oracle Linux / RHEL / Arch)

Kafka + daemon en **un solo compose Podman**, systemd como usuario **`datalake`**. Un solo dueño de `:9092` — sin mezclar tu usuario personal con systemd.

## Checklist antes de instalar

| # | Requisito | Comando / nota |
|---|-----------|----------------|
| 1 | **Oracle Linux 8/9** (o RHEL) con `podman` + compose v2 | `podman --version` / `podman compose version` |
| 2 | Usuario **`datalake`** | `id datalake` — `install-systemd.sh` lo crea si falta |
| 3 | Repo en **`/opt/DataSync`** (o ruta fija) | `git clone` ahí |
| 4 | **`config.json`** con PostgreSQL alcanzable | copiar de `config.json.example` |
| 5 | Puerto **9092 libre** | `ss -tlnp \| grep 9092` — vacío antes de instalar |
| 6 | **`sudo`** para instalar systemd (una vez) | no corras `podman compose` como tu user en prod |
| 7 | PostgreSQL accesible desde el host | `config.json` → `datasync` / `datalake` |

## Instalación prod (copy-paste)

```bash
# Como tu usuario (ej. tomy.berrios) — NO sudo git
cd /opt/DataSync
git pull

# Parar contenedores viejos de TU podman (importante)
podman compose stop kafka datasync 2>/dev/null || true
podman compose rm -f kafka datasync 2>/dev/null || true

# Instalar / actualizar (sudo una vez por deploy)
./install.sh
```

`./install.sh` con sudo disponible ejecuta `install-systemd.sh`, que:

1. `chown datalake:datalake` el repo  
2. `loginctl enable-linger datalake` + `podman.socket`  
3. Limpia `:9092` (docker/podman viejos + units legacy)  
4. Instala unit **`DataSync.service`** (Kafka + daemon en compose)  
5. Levanta el stack como **datalake**

## Después de cada `git pull`

```bash
cd /opt/DataSync
git pull
./install.sh
```

No hace falta `systemctl restart` manual — `install.sh` re-sincroniza systemd.

## Verificar

```bash
./deploy/validate-stack.sh

systemctl status DataSync
bash -c 'echo >/dev/tcp/127.0.0.1/9092' && echo "Kafka OK"

DATASYNC_UID=$(id -u datalake)
sudo -u datalake env \
  XDG_RUNTIME_DIR=/run/user/${DATASYNC_UID} \
  DOCKER_HOST=unix:///run/user/${DATASYNC_UID}/podman/podman.sock \
  podman compose -f /opt/DataSync/docker-compose.yml ps
```

## Restart manual

```bash
sudo systemctl restart DataSync
```

## Logs

```bash
journalctl -u DataSync -n 50 --no-pager

# contenedores (como datalake)
sudo -u datalake env XDG_RUNTIME_DIR=/run/user/$(id -u datalake) \
  DOCKER_HOST=unix:///run/user/$(id -u datalake)/podman/podman.sock \
  sh -c 'cd /opt/DataSync && podman compose logs -f kafka datasync'
```

## Errores comunes

| Síntoma | Causa | Fix |
|---------|-------|-----|
| `bind: address already in use :9092` | Kafka en podman de otro usuario | `podman compose stop` como ese user + `./install.sh` |
| `need podman or docker` en systemd | socket datalake apagado | `sudo ./deploy/systemd/install-systemd.sh` |
| `203/EXEC` | units viejas | `git pull && sudo ./deploy/systemd/install-systemd.sh` |
| DataSync `disabled` | falló start anterior | `sudo systemctl enable --now DataSync` |
| Kafka OK en host, daemon no conecta | rootless Podman + kafka bridge / datasync host | compose: **ambos `network_mode: host`** |
| `Permission denied` leyendo config.json | SELinux en Oracle/RHEL | volúmenes con `:z` en compose; `restorecon` |
| `active_total=0` / `no_tables` | catálogo vacío o tier incorrecto | `psql -d datasync -f deploy/diagnose-catalog.sql` |
| Dos podman (tu user + datalake) | compose manual + systemd | parar tu compose; solo systemd datalake |

## Validación compose (qué monta cada servicio)

| Recurso | Origen | Contenedor |
|---------|--------|------------|
| `config.json` | host `./config.json` | `/app/config.json` (ro) |
| SQL | host `./sql/` | `/app/sql/` (ro) |
| Binario CDC | imagen `datasync:local` | `/usr/local/bin/DataSync` |
| Bootstrap Python | imagen (build) | `/app/docker/*.py` |
| Kafka | imagen `cp-kafka:7.6.1` | host `:9092` |
| PG / MariaDB | **no en compose** | vía `config.json` + red host |

Variables críticas en `datasync`:

- `DATASYNC_CONFIG=/app/config.json`
- `DATASYNC_HOST_NETWORK=1` — PG/MariaDB en `localhost` del host
- `KAFKA_BOOTSTRAP=127.0.0.1:9092`
- `DATASYNC_RUN_MIGRATIONS=0` en daemon (schema solo en `./install.sh`)

`/etc/datasync/datasync.env` afecta scripts systemd, **no** el contenedor (salvo rebuild con env_file).

## Dev local (sin systemd)

```bash
SKIP_SYSTEMD=1 ./install.sh
podman compose ps
```
