-- cutover_drop_row_hash.sql — one-time, DESTRUCTIVE cutover for Pass B.
--
-- Pass B swapped every omicidx loader from the old merge_to_ducklake path
-- (which carried a payload `_row_hash` column) to cdsci-lake's `upsert`,
-- which gates UPDATEs on IS DISTINCT FROM and needs no hash column. The
-- existing lake tables still carry the now-dead `_row_hash` column; this
-- script drops it.
--
-- RUN DELIBERATELY, ONCE PER ENVIRONMENT, GATED ON THE NEW UPSERT PATH BEING
-- LIVE:
--   1. Deploy the Pass B loaders and let a load run clean against `omicidx_dev`.
--   2. Run this against `omicidx_dev` FIRST, verify, then run against prod
--      (`omicidx`).
--   3. This DROP COLUMN is irreversible — the column data is gone. It only
--      makes sense after the loaders that USED `_row_hash` are retired.
--
-- Attach the lake catalog as `lake` (see config.get_ducklake_connection /
-- get_lake_connection), point the schema below at the target environment,
-- and execute. Do NOT run as part of a normal load.
--
-- Swap `omicidx` → `omicidx_dev` to target the dev schema.

ALTER TABLE lake.omicidx.bioproject      DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.biosample       DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.ebi_biosample   DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.geo_series      DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.geo_sample      DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.geo_platform    DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.sra_study       DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.sra_sample      DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.sra_experiment  DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.sra_run         DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.sra_accessions  DROP COLUMN IF EXISTS _row_hash;
ALTER TABLE lake.omicidx.pubmed_article  DROP COLUMN IF EXISTS _row_hash;
