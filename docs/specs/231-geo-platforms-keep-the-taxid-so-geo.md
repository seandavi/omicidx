# Spec: GEO platforms keep their taxid, so GPL organism names resolve too (#231)

Issue: <https://github.com/seandavi/omicidx/issues/231>. Branch:
`issue-231-geo-platforms-keep-the-taxid-so-geo`. Follow-up to #226 / PR #230.

Design record read: `AGENTS.md` (= `CLAUDE.md`), `README.md`,
`docs/specs/omicidx-deliverables.md`, `docs/adrs/0003` and `0004`,
`packages/omicidx-prefect/src/omicidx/prefect/transform/README.md`,
`systemd/README.md`, `docs/overview/data-sources.qmd`. Departures are listed
under "Departures from the design record".

## Problem

PR #230 resolves organism names by taxid against current NCBI Taxonomy in
`stg.geo_series`, `stg.geo_samples`, `stg.sra_samples` and `stg.biosamples`.
GEO platforms are not covered. `_parse_single_gpl_soft`
(`packages/omicidx-parsers/src/omicidx/parsers/geo/parser.py`) joins
`!Platform_taxid` into a string, and `GEOPlatform` has no `taxid` field, so
pydantic drops it. `lake.omicidx.geo_platform`, `geo_platforms.parquet` and
`geometadb.gpl` therefore carry the submitted names only.

Facts from the published `latest/geo_platforms.parquet` (anonymous HTTPS,
2026-10-27):

- 29,152 platforms. The earliest `last_update_date` is 2005-05-26, so the GEO
  month range that starts at `2005-01` covers every platform.
- 1,717 platforms list more than one organism. `organism` holds them
  newline-joined, as `channels[].organism` does for GSM. `!Platform_taxid` has
  one line per organism, in the same order.

GEO renames organisms without bumping `last_update_date`, so the incremental
extract will never re-fetch the old platforms. Every GPL has to be fetched
again once.

## Goal

1. `GEOPlatform` gains `taxid: list[int]`, which the parser fills from
   `!Platform_taxid`.
2. One operator command re-extracts **only** the GPLs (about 29k fetches,
   roughly 15 to 20 minutes). It rewrites each month's
   `geo/raw/gpl/year=…/month=…/data_0.ndjson.gz` and leaves the GSE/GSM files
   and the semaphores alone.
3. `lake.omicidx.geo_platform` gains a `taxid BIGINT[]` column (appended last).
4. `stg.geo_platforms` resolves `organism` by taxid in the same
   `*_as_submitted` pattern as `stg.geo_samples`' channels. `geo_platforms.parquet`
   and `geometadb.gpl` read it.

## Non-goals

- No `taxid` column in `geometadb.gpl`. `geometadb.gsm` exposes no taxid
  either, and nobody has asked for one. It is still in `geo_platforms.parquet`.
- No change to `geometadb.gse_gpl`. It reads only `accession`/`series_id` from
  `src.geo_platforms`.
- No full `--force` GEO re-extract. A full 2005-to-now pass is more than 30 hours
  of NCBI load (`systemd/README.md` quotes about 20 hours for 2005–2020 alone),
  and it only refreshes GSE/GSM, which #226 already resolves in `stg`.
- No changes to the legacy `omicidx-etl` GEO extractor. It imports the same
  parser, so it picks up the new field without any work.
- No new timer. The command is a one-off recovery tool, run by hand under the
  GEO lock.

## Design

### 1. Parser and model (`omicidx-parsers`)

`packages/omicidx-parsers/src/omicidx/parsers/geo/pydantic_models.py`, in
`GEOPlatform`, directly after `organism`:

```python
    organism: str | None = None
    taxid: list[int] = []
```

`organism` stays a newline-joined `str`. That is the published contract, and it
matches `GEOChannel.organism`/`GEOChannel.taxid`.

`packages/omicidx-parsers/src/omicidx/parsers/geo/parser.py`:

- `_parse_single_entity_soft`: add `"taxid"` to `INT_FIELDS`, giving
  `["pubmed_id", "sample_taxid", "platform_taxid", "taxid"]`. After prefix
  stripping, the bare key `taxid` comes only from `!Platform_taxid`. GSM uses
  `taxid_ch1`/`taxid_ch2`, and GSE uses `sample_taxid`/`platform_taxid`. The
  existing `try/except (TypeError, ValueError) -> []` handles a non-integer
  value.
- `_parse_single_gpl_soft`: remove `"taxid"` from the join list, leaving
  `["technology", "distribution", "organism", "data_row_count"]`. A GPL with no
  `!Platform_taxid` line has no `taxid` key, so the model default `[]` applies.

### 2. GPL-only re-extract (`flows/geo.py`)

`packages/omicidx-prefect/src/omicidx/prefect/flows/geo.py`:

- `_fetch_accessions(start_date: date, end_date: date, etyps: tuple[str, ...] = ("GSM", "GSE", "GPL")) -> list[str]`.
  The term's first clause becomes
  `"(" + " OR ".join(f"{e}[etyp]" for e in etyps) + ")"`. With the default it
  produces exactly today's `(GSM[etyp] OR GSE[etyp] OR GPL[etyp])`.
  `extract_month` keeps calling it positionally as `(start_date, end_date)`, so
  the existing test stubs `lambda start, end: …` still work.
- New `_raw_path(entity: str, start_date: date) -> UPath` returns
  `get_upath("geo", "raw") / entity / f"year={start_date.strftime('%Y')}" / f"month={start_date.strftime('%m')}" / "data_0.ndjson.gz"`.
  `extract_month` uses it in place of its inline path expression, so the
  re-extract cannot write to a different layout.
- New public function:

  ```python
  def reextract_platforms(
      start_month: str = "2005-01", end_month: str | None = None
  ) -> dict[str, int]:
      """Re-fetch every GPL and rewrite each month's gpl/ file (#231).

      Same search, fetch, parse and failure guard as `extract_month`, limited to
      GPL[etyp]. GSE/GSM files and semaphores are untouched. Returns
      {month key: GPLs written}.
      """
  ```

  For each `key` in `_enumerate_months(start_month, end_month)`:
  `start_date, end_date = _month_range(...)`;
  `accessions = asyncio.run(_fetch_accessions(start_date, end_date, etyps=("GPL",)))`.
  If there are accessions, run `parsed, failed = asyncio.run(_fetch_and_parse(accessions))`
  and raise `RuntimeError` when `failed > len(accessions) * MAX_FETCH_FAILURE_RATE`,
  with a message that names the month. Otherwise set `parsed = {"GPL": []}`. Then
  `counts[key] = _write_ndjson_gz(parsed["GPL"], _raw_path("gpl", start_date))`.
  An empty month still writes an empty file, as `extract_month` does. Log one
  INFO line per month. Months are processed one at a time, because
  `MAX_WORKERS = 1` applies here for the same reason it does in the extract.
  A run that raises partway through leaves the months it finished rewritten.
  That is safe: re-run with `--start-month` set to the failed month.
- The logging setup in `run_command` (the `basicConfig(..., force=True)` and
  the `httpx` WARNING line, with their comments) moves into a
  `_configure_logging() -> None` helper. `run_command` and the new command both
  call it.
- New click command on the module's existing `cli` group:

  ```python
  @cli.command("reextract-platforms")
  @click.option("--start-month", default="2005-01", show_default=True)
  @click.option("--end-month", default=None, help="Defaults to the current month.")
  def reextract_platforms_command(start_month: str, end_month: str | None) -> None:
      """Re-fetch every GEO platform (GPL) and rewrite the monthly gpl/ files."""
  ```

  It calls `_configure_logging()` and `reextract_platforms(...)`, then
  `click.echo(f"geo: {sum(counts.values()):,} platforms re-extracted over {len(counts)} months")`.

No `omicidx-prefect run` subcommand is added. Those mirror timers
(`AGENTS.md`), and this command has no timer.

Overwriting a month's GPL file with a GPL-only search gives the same GPL set
that a forced full extract of that month would write today. A GPL whose update
date has since moved to a later month drops out of the old month's file and
appears in the new one. The loader's `QUALIFY` (latest `last_update_date` wins)
already handles stale copies left in other months.

### 3. Lake load (`flows/ducklake_geo.py`)

- Add `CAST(taxid AS BIGINT[]) AS taxid,` to `_GEO_PLATFORM_SOURCE` as the
  **last** projected column, after `trim(manufacture_protocol) AS manufacture_protocol`
  and before `row_number() … AS rn`. Both requirements were verified on DuckDB
  1.5.3 / DuckLake, the version in `uv.lock`:
  - The column must come **last** because `upsert`'s `MERGE … UPDATE SET * /
    INSERT *` matches columns **by position**, not by name. A reordered source
    raised `ConversionException: … cannot be cast … BIGINT[]`. `ALTER TABLE ADD
    COLUMN` appends, so the source must append too.
  - The value needs `CAST` because a file whose platforms all have `taxid: []`
    makes `read_ndjson_auto` infer `JSON[]`. Old raw files without the key yield
    `NULL` under `union_by_name = true`.
- New module constant and its use in `geo_platform_to_ducklake`, before
  `_merge_geo`:

  ```python
  # #231 added taxid. `upsert` creates a table but never alters one, and its MERGE
  # is positional, so an existing geo_platform gets the column appended here (a
  # no-op once it exists; IF EXISTS covers a fresh lake, which upsert creates
  # with taxid already last).
  _GEO_PLATFORM_ADD_TAXID = (
      "ALTER TABLE IF EXISTS {target} ADD COLUMN IF NOT EXISTS taxid BIGINT[]"
  )
  ```

  ```python
      target = f"lake.{lake_schema}.geo_platform"
      with get_lake_connection() as con:
          con.execute(_GEO_PLATFORM_ADD_TAXID.format(target=target))
      return _merge_geo(...)  # unchanged arguments
  ```

  DuckLake supports `ALTER TABLE IF EXISTS … ADD COLUMN IF NOT EXISTS`. This was
  verified on DuckDB 1.5.3: repeat calls and a missing table are both no-ops,
  and a subsequent `upsert` succeeds. The ALTER does not go through `ops.run`,
  so the ALTER snapshot is unattributed. It happens once.
- Update the module docstring's geo_platform line to mention `taxid`.

### 4. SQLMesh transform

- `transform/external_models.yaml`: under `"lake"."omicidx"."geo_platform"`,
  append `taxid: BIGINT[]` after `manufacture_protocol: TEXT`. Without it,
  SQLMesh's `SELECT *` expansion would leave out the column.
- `transform/models/stg/geo_platforms.sql` keeps the same `MODEL` header, and
  the body becomes:

  ```sql
  -- The lake table, column for column, with organism renamed to current NCBI
  -- Taxonomy by taxid (#231; GEO renames without bumping last_update_date). A
  -- platform's organism is its organisms newline-joined, aligned with its taxid
  -- list; unresolvable taxids, NULL taxid (rows not yet re-extracted) and
  -- misaligned lists keep the submitted name, which rides along as
  -- organism_as_submitted.
  WITH platform_taxa AS (
      SELECT
          accession,
          unnest(taxid) AS taxid,
          unnest(string_split(organism, chr(10))) AS organism,
          generate_subscripts(taxid, 1) AS i
      FROM src.geo_platforms
      WHERE len(taxid) = len(string_split(organism, chr(10)))
  ),
  resolved AS (
      SELECT
          t.accession,
          string_agg(coalesce(n.scientific_name, t.organism), chr(10) ORDER BY t.i)
              AS organism
      FROM platform_taxa t
      LEFT JOIN stg.ncbi_taxonomy_names n ON n.tax_id = t.taxid
      GROUP BY t.accession
  )
  SELECT
      g.* REPLACE (coalesce(r.organism, g.organism) AS organism),
      g.organism AS organism_as_submitted
  FROM src.geo_platforms g
  LEFT JOIN resolved r ON r.accession = g.accession;
  ```

  This was checked in DuckDB 1.5.3 against these cases: resolved, multi-organism
  with an unknown taxid, `NULL` taxid, length mismatch, and `NULL` organism with
  `[]` taxid.
- `transform/models/geometadb/gpl.sql`: `FROM src.geo_platforms` becomes
  `FROM stg.geo_platforms`, and `organism_as_submitted` is appended as the last
  select item. Every existing column name stays the same.

### 5. Parquet export (`flows/parquet_export.py`)

Add `"geo_platform": "geo_platforms"` to `TAXONOMY_RESOLVED_EXPORTS`. As a
result, `geo_platforms.parquet` gains `taxid` and `organism_as_submitted`
(additive), and its `organism` carries current names.

### 6. Docs

- `docs/overview/data-sources.qmd` line 28: add `geo_platforms` to the list of
  resolved tables, and replace the sentence "`geo_platforms` has no taxonomy
  ID, so platform organisms stay as submitted." with one saying that platforms
  with several organisms are resolved per organism (newline-joined), in
  `organism`, with `organism_as_submitted`.
- `transform/README.md`, "Organism names (#226)": add `geo_platforms` to the
  stg views, change "four base files" to "five", and replace the last sentence
  (GPL names stay as submitted) with one pointing to #231.
- `systemd/README.md`, "GEO: never stack two extracts": add the
  `reextract-platforms` form of the `systemd-run … flock …` example and one
  sentence saying it is how to re-fetch every GPL after a GPL parser change
  (#231).

### 7. Tests that prove it

| Test (file) | Proves |
|---|---|
| `test_iter_soft_entities_platform_taxid` (`packages/omicidx-parsers/tests/geo/test_geo_soft_parsers.py`) | GPL4 from `fixtures/GSE10.soft` parses with `taxid == [9606]` and `organism == "Homo sapiens"`. |
| `test_parse_gpl_taxid_multi_and_missing` (same file) | An inline `^PLATFORM` block with two `!Platform_organism`/`!Platform_taxid` pairs gives `taxid == [9606, 10090]` and `organism == "Homo sapiens\nMus musculus"`. A block with no taxid line gives `taxid == []`. A block with `!Platform_taxid = abc` gives `[]`. |
| `test_reextract_platforms_rewrites_only_gpl_files` (`packages/omicidx-prefect/tests/test_geo.py`) | This test uses `_stub_env`, a `_fetch_accessions` stub that records `etyps`, and a `_fetch_soft` stub that returns a GPL SOFT block with `!Platform_taxid = 9606`. `reextract_platforms("2020-07", "2020-07")` returns `{"2020-07": 1}`, the recorded `etyps == ("GPL",)`, and the gzip'd `gpl/year=2020/month=07/data_0.ndjson.gz` holds one record with `taxid == [9606]`. No `gse/` or `gsm/` file is written, and `SemaphoreStore("geo").exists("2020-07")` is false. |
| `test_geo_platform_load_adds_taxid_last` (new `packages/omicidx-prefect/tests/test_ducklake_geo.py`) | This test uses a local file-catalog DuckLake (import `_local_lake` from `.test_export_steel_thread`, which skips offline) and two local NDJSON files: an old-shape record without `taxid` (GPL1) and two new records, `[9606]` (GPL2) and `[]` (GPL3). It creates the pre-#231 table as `CREATE TABLE lake.omicidx.geo_platform AS SELECT * EXCLUDE (taxid) FROM (<_GEO_PLATFORM_SOURCE over the files>) WHERE false`, executes `_GEO_PLATFORM_ADD_TAXID` twice, and executes it against a missing table (no error). It then runs `upsert(con, target, _GEO_PLATFORM_SOURCE.format(path=glob), key="accession")`. The last `DESCRIBE` row must be `taxid BIGINT[]`, and the rows must be GPL1 `NULL`, GPL2 `[9606]` and GPL3 `[]`. |
| `test_stg_views_resolve_organism_names_by_taxid` (`packages/omicidx-prefect/tests/test_ncbi_taxonomy.py`, extended) | This adds a `src.geo_platforms(accession, organism, taxid)` table and `geo_platforms` to the stg build loop. It asserts `(organism, organism_as_submitted)` for these cases: a single taxid that resolves; a merged taxid (999 → 729); a multi-organism platform with an unknown taxid (`[72520, 5]`), which becomes `'Microchloropsis gaditana\ny'`; `NULL` taxid, which keeps the submitted name; and a length mismatch, which keeps the submitted name. |
| `test_mart_schema_resemblance` (`packages/omicidx-prefect/tests/test_marts_schema.py`, updated) | A new `stg.geo_platforms` DDL contains the columns `gpl.sql` reads plus `organism_as_submitted VARCHAR`. `src.geo_platforms` shrinks to `(accession VARCHAR, series_id VARCHAR[])`, the columns `gse_gpl` reads. `EXPECTED["geometadb.gpl"]` gains `"organism_as_submitted"`. |

## Acceptance criteria

- [ ] `GEOPlatform` has `taxid: list[int] = []`, and parsing GPL4 from
      `GSE10.soft` yields `taxid == [9606]`.
- [ ] Multi-organism GPLs yield an ordered `taxid` list. Missing or non-integer
      `!Platform_taxid` yields `[]`. `organism` stays newline-joined.
- [ ] `python -m omicidx.prefect.flows.geo reextract-platforms --help` lists
      `--start-month` (default `2005-01`) and `--end-month`.
- [ ] `reextract_platforms` searches `GPL[etyp]` only, rewrites only `gpl/`
      month files, writes no semaphore, and raises when a month exceeds
      `MAX_FETCH_FAILURE_RATE`.
- [ ] `_fetch_accessions`' default term is unchanged, and the existing
      `test_geo.py` tests pass without edits.
- [ ] `_GEO_PLATFORM_SOURCE` projects `CAST(taxid AS BIGINT[]) AS taxid` as its
      last column, and `geo_platform_to_ducklake` runs
      `_GEO_PLATFORM_ADD_TAXID` before the MERGE.
- [ ] `external_models.yaml` declares `taxid: BIGINT[]` last on `geo_platform`.
- [ ] `stg.geo_platforms` is `src.geo_platforms` column for column plus
      `organism_as_submitted`, with `organism` resolved per element.
- [ ] `geometadb.gpl` reads `stg.geo_platforms` and adds
      `organism_as_submitted`. No existing column changes name.
- [ ] `TAXONOMY_RESOLVED_EXPORTS` includes `"geo_platform": "geo_platforms"`.
- [ ] Docs updated (`data-sources.qmd`, `transform/README.md`, `systemd/README.md`).
- [ ] `uv run ruff check`, `uv run ruff format --check`, `uv lock --locked`,
      and `uv run pytest packages/omicidx-parsers/tests/geo/test_geo_soft_parsers.py packages/omicidx-prefect/tests/`
      all pass.
- [ ] **Operator, after merge** (out of the PR; listed so the PR body carries
      it):
      1. `git pull` in `/home/davsean/Documents/git/omicidx`.
      2. Before the next `omicidx-downstream` run, re-extract under the GEO lock:
         ```bash
         systemd-run --user --unit=omicidx-geo-reextract-gpl -p WorkingDirectory=$PWD \
           -p EnvironmentFile=$PWD/.env /usr/bin/flock /run/user/$UID/omicidx-geo.lock \
           ~/.local/bin/uv run python -m omicidx.prefect.flows.geo reextract-platforms
         ```
      3. Let `omicidx-downstream` run, or run `omicidx-prefect run ducklake-load`
         → `transform` → `parquet-export` → `publish-bundle` by hand.
      4. Check: in `lake.omicidx.geo_platform`, `count(*) FILTER (taxid IS NULL)`
         is at most a handful (withdrawn or failed GPLs). In the published
         `geo_platforms.parquet`, `count(*) FILTER (organism <> organism_as_submitted)`
         is reported in the issue.

## Test plan

1. Unit tests (offline): the six tests in §7 run in the `pytest` command above.
   `test_ducklake_geo.py` needs the `ducklake` extension and skips offline, as
   `test_export_steel_thread.py` does.
2. Lint and lock: `uv run ruff check packages/`, `uv run ruff format --check packages/`,
   `uv lock --locked`.
3. Live verification is an operator step (the acceptance checklist above). A
   smoke run before merge is allowed but optional: `reextract-platforms
   --start-month 2012-04 --end-month 2012-04` writes a single month's GPL file
   and is idempotent. Run it under the GEO lock. CI does not run it.

## Risks

- **Downstream between pull and re-extract.** Before any raw `gpl/` file
  contains a `taxid` key, `_GEO_PLATFORM_SOURCE` fails with
  `BinderException: Referenced column "taxid" not found`, and the
  `ducklake-load` failure trips `OnFailure=`. The nightly GEO timer starts writing
  `taxid` into the current month's file as soon as the new code runs, which
  usually closes this window by itself. Mitigation: run step 2 right after the
  pull, and re-run `ducklake-load` if a downstream run fell inside the window.
- **Positional MERGE.** Any later reorder of `_GEO_PLATFORM_SOURCE` that moves
  `taxid` off the end breaks the load, either with a cast error or with silently
  swapped same-typed columns. `test_geo_platform_load_adds_taxid_last` guards
  this.
- **Fetch failures under the 2% guard.** A GPL that fails to fetch drops out of
  its month's rewritten file. The lake row survives, because `upsert` never
  deletes, but it keeps `taxid = NULL` and therefore its submitted name. The
  same exposure exists in `extract_month`. The operator count in step 4 makes
  it visible.
- **One-time full MERGE update.** About 29k `geo_platform` rows change once
  (`taxid` goes from `NULL` to a value), producing one ordinary DuckLake
  snapshot.
- **Organism/taxid misalignment** in a SOFT record (counts differ) keeps the
  submitted name. This is the same guard as in `stg.geo_series`/`stg.geo_samples`.

## Departures from the design record

- **First lake schema evolution done in a loader.** No loader has run `ALTER
  TABLE` before. `upsert` (cdsci-lake) only creates tables. The idempotent
  `ADD COLUMN IF NOT EXISTS` sits inside `geo_platform_to_ducklake`, not in an
  operator runbook. It therefore also covers non-prod `lake_schema` targets and
  costs nothing after the first run.
- **Additive public columns.** `geo_platforms.parquet` gains `taxid` and
  `organism_as_submitted`, and `geometadb.gpl` gains `organism_as_submitted`.
  Under ADR-0003 this is a minor (additive) change, and the ADR-0004 flat-Parquet
  plus views contract holds. #230 set the same precedent.
- **The sentence "geo_platforms has no taxonomy ID"** in
  `docs/overview/data-sources.qmd` and `transform/README.md` (written for #230)
  is now false and is replaced (§6).
