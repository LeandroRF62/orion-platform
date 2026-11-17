-- Referência. O ingest_incremental.py já cria tudo isto sozinho na
-- primeira execução (ALTER TABLE ... ADD COLUMN IF NOT EXISTS), então
-- rodar à mão é opcional.

CREATE TABLE IF NOT EXISTS sync_state (
    sensor_id      BIGINT PRIMARY KEY,
    last_timestamp TIMESTAMP
);

ALTER TABLE devices ADD COLUMN IF NOT EXISTS reference     TEXT;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS modelo        TEXT;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS firmware      TEXT;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS fuso_horario  TEXT;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS projeto       TEXT;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS site          TEXT;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS intervalo_min INTEGER;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMPTZ;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS raw           JSONB;

ALTER TABLE sensores ADD COLUMN IF NOT EXISTS canal          INTEGER;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS canal_device   INTEGER;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS scada_tag      TEXT;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS sensor_base_id BIGINT;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS eh_soft        BOOLEAN;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS oculto         BOOLEAN;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS notas          TEXT;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS ultimo_valor   DOUBLE PRECISION;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS atualizado_em  TIMESTAMPTZ;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS raw            JSONB;

CREATE INDEX IF NOT EXISTS idx_sensores_device ON sensores(device_id);
CREATE INDEX IF NOT EXISTS idx_leituras_sensor_data
    ON leituras(sensor_id, data_leitura DESC);

CREATE TABLE IF NOT EXISTS alert_status_log (
    device_id          BIGINT PRIMARY KEY,
    ultimo_tarp        TEXT,
    ultimo_status      TEXT,
    ultima_atualizacao TIMESTAMPTZ
);


-- ---------------------------------------------------------------------
-- Semear os cursores para evitar backfill desde 2025 em sensores novos.
-- Rode UMA vez, antes da primeira execução da versão corrigida.
-- ---------------------------------------------------------------------
INSERT INTO sync_state (sensor_id, last_timestamp)
SELECT sensor_id, TIMESTAMP '2026-01-01 00:00:00'
FROM sensores
WHERE tipo_sensor IN (
        'A-Axis Delta Angle', 'B-Axis Delta Angle', 'Air Temperature',
        'Device Temperature', 'Water Level', 'Water Temperature', 'Rainfall',
        'VWP (Frequency)', 'VWP (Temp)')
  AND sensor_id NOT IN (SELECT sensor_id FROM sync_state)
ON CONFLICT (sensor_id) DO NOTHING;


-- ---------------------------------------------------------------------
-- Diagnóstico rápido: a ingestão está em dia?
-- ---------------------------------------------------------------------
-- SELECT max(data_leitura) FROM leituras;
-- SELECT count(*) FILTER (WHERE last_upload > now() - interval '2 days'),
--        count(*) FROM devices;
