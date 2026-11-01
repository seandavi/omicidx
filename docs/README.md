# OmicIDX docs

[Quarto](https://quarto.org) website, served at https://omicidx.cancerdatasci.org
by the Worker in `../site/`. The site is for people using the data;
contributor docs live in the repo (`CLAUDE.md`, `systemd/README.md`, the
package READMEs). `adrs/`, `specs/`, `research/` and the other `.md` files here
are contributor notes and aren't rendered (`render:` in `_quarto.yml`).

## Layout

```
_quarto.yml        # site config: navbar, sidebar, theme, GA4, freeze
index.qmd          # landing page
overview/          # what OmicIDX is, sources, architecture
guides/            # get the data, DuckDB, R (omicidxr), versions and citation
playground.qmd     # DuckDB-Wasm SQL box against latest/ (OJS)
releases.qmd       # live list of latest/, dated folders, HF monthlies
shell-links.html   # adds "Open in DuckDB Shell" under every SQL block
theme/             # archify*.scss copied from bioc-infrastructure; omicidx.scss
_freeze/           # stored example outputs (committed)
```

## Executed examples

R, Python and SQL blocks run at render time, against live data, so a page
can't show output it didn't produce. Use `latest/` (HuggingFace rate-limits
anonymous reads, so HF examples are plain, non-executed blocks), keep queries
small (`LIMIT`, selective filters), and never hard-code a dated folder in an
executed block: most are deleted after a week (ADR-0007).

- **SQL:** ```` ```{sql} ```` blocks run on `con`, a DuckDB connection the page opens in a hidden setup chunk (`connection: con` in `_quarto.yml`). Mark a non-executed SQL block that shouldn't get a shell link with ```` ```{.sql .no-shell} ````.
- **Python:** runs through reticulate; `reticulate::py_require()` in the page's setup chunk provisions duckdb/polars/pyarrow with uv.
- **R:** needs `omicidxr` (`pak::pak("seandavi/omicidxr")`), duckdb, DBI, dplyr, dbplyr, knitr, rmarkdown, reticulate.

`freeze: auto` re-runs a page only when its source changes, and stores the
results in `_freeze/`. Commit `_freeze/` with the page, so CI and deploys
render without R, Python or the data.

```bash
quarto preview docs                  # local preview, re-renders on save
quarto render docs                   # build _site/ (executes changed pages)
rm -rf docs/_freeze && quarto render docs   # re-run every example
```

CI renders the site from `_freeze/` on every push (`docs` job in
`ci.yaml`), and `docs-examples.yaml` re-runs every example weekly against
live data, failing on any broken one.

## Hosting

The Worker in `../site/` serves `docs/_site/` as Workers static assets. To
publish:

```bash
quarto render docs && cd site && npx wrangler deploy
```

Old Starlight URLs (`/guides/duckdb-snapshot/`) still resolve: Workers assets'
default `auto-trailing-slash` handling redirects them to
`/guides/duckdb-snapshot`, which serves `guides/duckdb-snapshot.html`.

`docs.omicidx.cancerdatasci.org` and the retired `api-omicidx.cancerdatasci.org`
301 to the new host via a Traefik file-provider rule on onclappc02
(`monode/infrastructure/compose/traefik/config/omicidx-redirects.yml`).
