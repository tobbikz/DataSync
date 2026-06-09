# DataSync — producción (Oracle Linux / RHEL / Arch)

Kafka + daemon en **Podman**, systemd como usuario **`datalake`**. Un solo dueño de `:9092` — sin mezclar tu usuario personal con systemd.

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
3. Limpia `:9092` (docker/podman viejos + Kafka nativo si existía)  
4. Instala units **DataSync-kafka** + **DataSync**  
5. Levanta Kafka (compose) + daemon (compose) como **datalake**

## Después de cada `git pull`

```bash
cd /opt/DataSync
git pull
./install.sh
```

No hace falta `systemctl restart` manual — `install.sh` re-sincroniza systemd.

## Verificar

```bash
systemctl status DataSync-kafka DataSync
bash -c 'echo >/dev/tcp/localhost/9092' && echo "Kafka OK"

DATASYNC_UID=$(id -u datalake)
sudo -u datalake env \
  XDG_RUNTIME_DIR=/run/user/${DATASYNC_UID} \
  DOCKER_HOST=unix:///run/user/${DATASYNC_UID}/podman/podman.sock \
  podman compose -f /opt/DataSync/docker-compose.yml ps
```

## Restart manual

```bash
sudo systemctl restart DataSync-kafka
sudo systemctl restart DataSync
```

## Logs

```bash
journalctl -u DataSync-kafka -n 50 --no-pager
journalctl -u DataSync -n 50 --no-pager

# contenedores (como datalake)
sudo -u datalake env XDG_RUNTIME_DIR=/run/user/$(id -u datalake) \
  DOCKER_HOST=unix:///run/user/$(id -u datalake)/podman/podman.sock \
  sh -c 'cd /opt/DataSync && podman compose logs -f kafka'
```

## Errores comunes

| Síntoma | Causa | Fix |
|---------|-------|-----|
| `bind: address already in use :9092` | Kafka en podman de otro usuario | `podman compose stop` como ese user + `./install.sh` |
| `need podman or docker` en systemd | socket datalake apagado | `sudo ./deploy/systemd/install-systemd.sh` |
| `203/EXEC` | units viejas | `git pull && sudo ./deploy/systemd/install-systemd.sh` |
| DataSync `disabled` | falló start anterior | `sudo systemctl enable --now DataSync` |

## Dev local (sin systemd)

```bash
SKIP_SYSTEMD=1 ./install.sh
podman compose ps
```
