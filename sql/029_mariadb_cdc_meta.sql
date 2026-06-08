-- MariaDB CDC heartbeat (native capture idle bump)
-- Applied by scripts/setup_all.sh on prod :3306

CREATE DATABASE IF NOT EXISTS cdc_meta;

CREATE TABLE IF NOT EXISTS cdc_meta.heartbeat (
    id      TINYINT NOT NULL PRIMARY KEY,
    ts      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    note    VARCHAR(100) NULL
);

INSERT INTO cdc_meta.heartbeat (id, note) VALUES (1, 'cdc heartbeat')
ON DUPLICATE KEY UPDATE note = VALUES(note);
