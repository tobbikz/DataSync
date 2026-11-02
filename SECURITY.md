# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| `main` / latest release | Yes |
| Older tags | Best effort |

## Reporting a vulnerability

Please **do not** file a public GitHub issue for security bugs.

Email **IKZ Technologies** via the contact on the GitHub org profile, or open a
**private** security advisory on this repository (GitHub → Security → Advisories).

Include:

- Affected component (`cpp/`, `ui/`, compose/install, …)
- Reproduction steps or PoC (non-destructive if possible)
- Impact assessment (auth bypass, data exposure, RCE, …)

We aim to acknowledge within a few business days and coordinate a fix + disclosure.

## Hardening tips for operators

- Never commit real `config.json` — copy from `config.json.example`.
- Set strong `DATASYNC_UI_PASSWORD` and `DATASYNC_UI_SECRET` in production.
- Do not expose Kafka (`9092`), the catalog DB, or the UI to the public internet without TLS and access control.
- Store source DB passwords in `cdc_catalog.connections` with least-privilege CDC accounts only.
- Rotate any lab defaults (`DataSync_Dev1!`, UI `admin` / `datasync`) before shared use.
