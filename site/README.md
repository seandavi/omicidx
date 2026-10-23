# omicidx site Worker

One Cloudflare Worker (named `data-omicidx`) serves the whole public site:

| URL | Serves |
|---|---|
| `https://omicidx.cancerdatasci.org/` | docs (the Quarto build in `../docs/_site`, Workers static assets) |
| `https://omicidx.cancerdatasci.org/data/*` | the `data-omicidx` R2 bucket, `/data` stripped from the key |
| `https://data.omicidx.cancerdatasci.org/*` | the same bucket at `/` |
| `https://data-omicidx.cancerdatasci.org/*` | retired legacy host (#211): 301 to `/data/latest/{basename}` if that file exists, else the docs home |

The data host must keep serving bytes, never redirect: published bundle
catalogs hard-code `https://data.omicidx.cancerdatasci.org/v{date}/data/` as
their `data_path`. Routing lives in `src/route.ts`.

The bucket paths are the anonymous, credential-free read surface for the frozen
omicidx bundle (deliverables spec §2 endpoint 2 / Stage B3): plain HTTPS with
range requests, CORS, directory listing and Analytics Engine logging — the
load-bearing read path for a DuckLake `ATTACH` of the published file catalog
with no client credentials.

## The read contract this enables

```sql
INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;
ATTACH 'ducklake:https://data.omicidx.cancerdatasci.org/latest/catalog.ducklake'
  AS omicidx (READ_ONLY);
SELECT * FROM omicidx.geo_platforms LIMIT 5;
```

The catalog's stored `data_path` is the absolute HTTPS URL of the bundle's
`v{date}/data/` folder, so DuckLake resolves data files over range GETs through
this Worker. No R2 credentials, no `--no-sign-request`, no signed URLs.

Flat Parquet still works the classic way:

```sql
SELECT * FROM read_parquet('https://data.omicidx.cancerdatasci.org/latest/sra_studies.parquet');
```

## Develop and deploy

```bash
cd site
npm ci
npm test               # routing unit tests
npx wrangler dev       # local Worker, real bucket (remote R2 binding)
npx wrangler login
npx wrangler deploy
```

There's no build step: `wrangler dev` and `wrangler deploy` serve whatever is
in `../docs/_site`, so run `quarto render docs` first (it renders from the
committed `_freeze/`; see `../docs/README.md`). All three custom domains are bound by the
`[[routes]]` blocks; `custom_domain = true` creates their DNS records. To try
the data host locally: `npx wrangler dev --local-upstream data.omicidx.cancerdatasci.org`.

## Verify anonymous attach end-to-end

After deploy, from any machine with DuckDB and **no credentials configured**:

```bash
duckdb -c "INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;
  ATTACH 'ducklake:https://<worker-host>/latest/catalog.ducklake' AS omicidx (READ_ONLY);
  SELECT count(*) FROM omicidx.geo_platforms;"
```
