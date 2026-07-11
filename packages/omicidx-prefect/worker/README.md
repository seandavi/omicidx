# data-omicidx Worker

Anonymous, credential-free public read surface for the frozen omicidx bundle
(deliverables spec §2 endpoint 2 / Stage B3). Serves the `data-omicidx` R2
bucket over plain HTTPS with range-request support and CORS — the load-bearing
read path for a DuckLake `ATTACH` of the published file catalog with no client
credentials.

Reuses the generic omicidx-data worker (range + CORS + directory listing +
Analytics Engine), retargeted to the `data-omicidx` bucket.

## The read contract this enables

```sql
INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;
ATTACH 'ducklake:https://data-omicidx.cancerdatasci.org/latest/catalog.ducklake'
  AS omicidx (READ_ONLY);
SELECT * FROM omicidx.geo_platforms LIMIT 5;
```

The catalog's stored `data_path` is the absolute HTTPS URL of the bundle's
`v{date}/data/` folder, so DuckLake resolves data files over range GETs through
this Worker. No R2 credentials, no `--no-sign-request`, no signed URLs.

Flat Parquet still works the classic way:

```sql
SELECT * FROM read_parquet('https://data-omicidx.cancerdatasci.org/latest/sra_studies.parquet');
```

## Deploy

```bash
cd worker
npm install
npx wrangler login
npx wrangler deploy
```

Create an Analytics Engine dataset `data_omicidx_usage` in the Cloudflare
dashboard (Account > Analytics > Analytics Engine). Bind the custom domain
`data-omicidx.cancerdatasci.org` (matching `PUBLIC_PARQUET_HTTPS_BASE`) via the
dashboard or the `[[routes]]` block in `wrangler.toml` once DNS is in place; the
r2.dev public dev URL works for verification in the meantime.

## Verify anonymous attach end-to-end

After deploy, from any machine with DuckDB and **no credentials configured**:

```bash
duckdb -c "INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;
  ATTACH 'ducklake:https://<worker-host>/latest/catalog.ducklake' AS omicidx (READ_ONLY);
  SELECT count(*) FROM omicidx.geo_platforms;"
```
