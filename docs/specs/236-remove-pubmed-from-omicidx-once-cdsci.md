# Spec: drop PubMed from the OmicIDX public release (#236, publication phase)

Issue: <https://github.com/seandavi/omicidx/issues/236>. Branch:
`issue-236-remove-pubmed-from-omicidx-once-cdsci`.

Design record read: `AGENTS.md` (= `CLAUDE.md`), `README.md`,
`docs/specs/omicidx-deliverables.md`, ADR-0004, ADR-0006, ADR-0007,
`packages/omicidx-prefect/{README.md,DUCKLAKE.md}`, `systemd/README.md`, the
public docs under `docs/`. Where this spec departs from them, it says so under
"Departures from the design record".

## Scope decision

The issue says to **wait** until cdsci-lake (seandavi/cdsci-lake#136, still
open on 2026-10-27) extracts, loads and publishes PubMed, and then remove
everything here. On 2026-10-27 the maintainer commented on #236: "Let's remove it
from publication now." This spec covers only that **publication** phase. It
removes `pubmed_articles` from everything OmicIDX publishes: the R2 `latest/`
and `v{date}/` folders, `omicidx.duckdb`/`views.sql`/`restore.sql`/`manifest.json`,
the HuggingFace monthly release, and the public docs. Everything that feeds
cdsci-lake's future takeover stays running until #136 ships. #236 stays open
for that second phase.

"Publication" here means the public OmicIDX release, the 37 GB of 73 GB that the
issue names. The `omicidx-pubmed` release (ADR-0006, `flows/publish_release.py`)
is not public: it writes to local storage only (a temp dir by default). It is
also the contract cdsci-lake#136 ports, so it stays.

## Goal

From the first nightly publish after merge, no OmicIDX public artifact
contains or references `pubmed_articles`. The GEO and SRA PMID columns
(`geometadb.gse.pubmed_id`, `sradb.study.pubmed_ids`, and the base
`geo_series`/`sra_studies` columns) are unchanged.

## Non-goals (phase 2 of #236, after cdsci-lake#136 ships)

- Removing the extract `flows/pubmed.py`, `omicidx-prefect run pubmed`, the
  systemd units `omicidx-pubmed-extract.{service,timer}`, or the `pubmed` entry
  in `flows/sources.py` / `scripts/validate_dev.py`.
- Removing the lake load `flows/ducklake_pubmed.py` (and its call in
  `ducklake_load.py`), or dropping `lake.omicidx.pubmed_article`.
- Removing the SQLMesh models `src.pubmed_articles` / `stg.pubmed_articles` and
  the `lake.omicidx.pubmed_article` entry in `transform/external_models.yaml`.
  They are internal views, and nothing in the public release reads them
  (`parquet_export` copies the lake table directly).
- Removing `flows/publish_release.py` / `omicidx-pubmed`, or marking ADR-0006
  superseded. Its decision still holds until cdsci-lake owns the release.
- Moving or deleting raw `r2://omicidx/pubmed/`.
- The legacy `omicidx-etl` `oidx pubmed` command.
- omicidxr: its source mentions PubMed only as the `gse.pubmed_id` /
  `sradb.study.pubmed_ids` columns, which stay. `pubmed_articles` appears only in
  generated pkgdown output (`docs/reference/omicidx_db.md`,
  `docs/articles/introduction.md`), which changes on its next site rebuild. No
  omicidxr change, and that repo is outside this worktree anyway.
- Pointing at a cdsci-lake PubMed dataset URL. None exists yet. The docs link
  to cdsci-lake#136 instead.
- Re-tagging or editing the HF `2026-10` release. It keeps
  `pubmed_articles.parquet` (verified: the `2026-10` tree lists it), and the
  docs name it as the last release with PubMed.

## Design

### 1. `packages/omicidx-prefect/src/omicidx/prefect/flows/parquet_export.py`

Delete the `("pubmed_article", "pubmed_articles.parquet")` entry from `EXPORTS`.
Nothing else changes. Everything else follows from `EXPORTS`:

- `flows/sql.py`: `PUBLIC_VIEWS` is built from `EXPORTS`, so `omicidx.duckdb`,
  `views.sql` and `restore.sql` lose the `main.pubmed_articles` view/table.
- `flows/publish_bundle.py`: `manifest.json` `tables` comes from
  `build_duckdb_local` summaries, so it loses the row.
- `flows/hf_release.py`: `download_release` copies whatever `*.parquet` is in
  `v{date}/`. New dated folders don't have the file, and `hf_release` already
  issues a `CommitOperationDelete` for every remote file not in the release. So
  the next monthly release (`2026-11`) removes `pubmed_articles.parquet` from
  `main`, and it stays reachable at tag `2026-10`. No code change for this.

### 2. `packages/omicidx-prefect/src/omicidx/prefect/flows/publish_bundle.py`

Delete `"pubmed"` from `RAW_NAMESPACES`. `raw_partition_counts` in the manifest
is the provenance of the publish, and PubMed raw no longer feeds any published
table. The docstring and comments need no edit.

### 3. `packages/omicidx-prefect/src/omicidx/prefect/flows/hf_release.py` (`build_card`)

Change the dataset card text, not its structure:

- Intro paragraph: replace "NCBI and EBI BioSample, BioProject, and PubMed." with
  "NCBI and EBI BioSample, and BioProject." Keep the rest of the sentence.
- After the intro paragraph that begins "This is the monthly release", add one
  paragraph:
  `PubMed articles (`pubmed_articles.parquet`) were published through release
  `2026-10` and are not in later releases. PubMed is moving to its own dataset
  (https://github.com/seandavi/cdsci-lake/issues/136).`
  Emit it unconditionally (a static string). Releases from before the removal
  already exist and are never regenerated.
- License paragraph: change "(SRA, GEO, BioSample, BioProject, PubMed)" to
  "(SRA, GEO, BioSample, BioProject)".

### 4. Tests (`packages/omicidx-prefect/tests/`)

- `test_bundle.py`: add
  ```python
  def test_pubmed_articles_is_not_published():
      """PubMed left the public release (#236); GEO/SRA keep their PMID columns."""
      assert all("pubmed" not in file for _, _, file in sql.PUBLIC_VIEWS)
  ```
  The `"pubmed" not in file` check runs on Parquet file names, so the PMID
  *columns* are not affected. Those are covered by `test_marts_schema.py`,
  which already asserts `pubmed_ids` / `pubmed_id` and is not changed.
- `test_bundle.py`: add a check that the manifest does not count PubMed raw:
  ```python
  def test_manifest_does_not_count_pubmed_raw():
      assert "pubmed" not in publish_bundle.RAW_NAMESPACES
  ```
  with `from omicidx.prefect.flows import publish_bundle` added to the imports.
- `test_hf_release.py::test_card_lists_tables_and_provenance`: append
  ```python
  assert "cdsci-lake/issues/136" in card
  assert "and PubMed" not in card  # intro source list
  assert "BioProject, PubMed" not in card  # license provider list
  ```

No other test changes. `test_restore_sql_materializes_and_checks_manifest`
expects the first table to be `main.bioprojects`, which is unchanged
(`bioproject` is the first entry of `EXPORTS`).

### 5. Docs (public site and repo README)

Remove `pubmed_articles` as a published table and say where PubMed went. The
standard wording, used once on the data-sources page and linked from elsewhere:

> PubMed is no longer part of the OmicIDX release. The last release with
> `pubmed_articles.parquet` is the HuggingFace monthly release `2026-10`. PubMed
> is moving to its own dataset in cdsci-lake
> ([seandavi/cdsci-lake#136](https://github.com/seandavi/cdsci-lake/issues/136)).
> GEO and SRA records still carry their PubMed IDs (`geometadb.gse.pubmed_id`,
> `sradb.study.pubmed_ids`).

| File | Change |
|---|---|
| `README.md` | Delete the `PubMed Articles` row of the table at line 78. |
| `docs/_quarto.yml` | Site description: drop ", and PubMed"; say "NCBI SRA, GEO, BioSample, and BioProject metadata". |
| `docs/index.qmd` | Front-matter description: "SRA, GEO, BioSample, and BioProject metadata as Parquet files ...". |
| `docs/overview/what-is-omicidx.qmd` | Delete the "**NCBI PubMed**" bullet. |
| `docs/overview/data-sources.qmd` | Delete the NCBI PubMed table row. Replace the "**PubMed:**" note with the standard wording above. |
| `docs/overview/architecture.qmd` | Stage 1 (Extract) source list: drop PubMed. Phase 2 removes the extract, and the public architecture page describes what feeds the release. |
| `docs/guides/duckdb-snapshot.qmd` | Line 38 table: drop `pubmed_articles` from the list. Lines 87–89: drop the sentence "`pubmed_articles.parquet` is about 37 GB, so filter it in place." Line 169: "A full copy is tens of GB, most of it `pubmed_articles` and `biosamples`." becomes "A full copy is tens of GB, most of it `biosamples`." Line 95 ("BioProject, GEO, and PubMed cross-references") stays: it describes the PMID columns. |
| `docs/playground.qmd` | Drop ", `pubmed_articles` 37 GB" from the big-tables list (keep `sra_runs` and `biosamples`). |
| `docs/guides/versions-and-citation.qmd` | No change: it names no table that leaves. |
| `docs/releases.qmd` | No change: built at view time from each folder's `manifest.json` and the HF tree. |

Do **not** edit: ADRs 0001/0002/0004 (historical context, and 0004's layout is
an illustrative example), `docs/v1.0-snapshot-README.md` and `docs/roadmap.md`
(historical plans), `docs/_freeze/**` (generated by Quarto; refreshed on the next
render), `docs/grok-review-e125249.md`.

### 6. Records

- `AGENTS.md`/`CLAUDE.md`: the `parquet-export` row of the stage table already
  describes the exports generically, and the extract row is still true. Add one
  sentence after the stage table: "PubMed is extracted and loaded into the lake
  but not published (#236). It moves to cdsci-lake (cdsci-lake#136)."
  `AGENTS.md` is a symlink to `CLAUDE.md`, so edit `CLAUDE.md`.
- ADR-0006: add a dated note under Consequences: "2026-10-27: `pubmed_articles`
  left the OmicIDX public bundle (#236). This release is now the only copy of
  PubMed that omicidx produces, until cdsci-lake#136 takes it over." Status
  stays Accepted. Supersession is phase 2.

### 7. Operator step after merge (not done by the implementer)

`parquet_export` only writes, so `latest/pubmed_articles.parquet` (37 GB) stays
on R2 after merge. Old `views.sql` copies would keep serving a frozen file.
Once the first post-merge nightly has published `latest/` without it (check
that `latest/manifest.json` has no `pubmed_articles` row), the maintainer
deletes the object:

```bash
uv run python -c "from omicidx.prefect.config import get_public_upath as p; f = p('latest', 'pubmed_articles.parquet'); f.fs.rm(f.path)"
```

Dated `v{date}/pubmed_articles.parquet` copies age out through the existing
30-day prune (`prune_dated_folders`). No extra deletion is needed. The PR
description lists this step. Deleting a public object is outward-facing, so the
implementer must not run it.

## Departures from the design record

- **Issue #236 ordering:** the issue says to wait for cdsci-lake#136 before
  removing anything. The maintainer's 2026-10-27 comment moves the publication
  removal ahead of that. Phase 2 (code, lake, raw, ADR-0006 supersession) still
  waits.
- **`docs/specs/omicidx-deliverables.md` (1):** it names PubMed as one of five
  sources into the internal lake. That stays true (PubMed is still extracted and
  loaded), so it gets no edit. The external artifact (2) no longer carries PubMed.
- **ADR-0004** lists `pubmed.parquet` in its example layout. ADRs record
  decisions at the time they were made, and the layout is illustrative, so it is
  not edited.

## Acceptance criteria

- [ ] `EXPORTS` in `flows/parquet_export.py` has no `pubmed_article` entry.
- [ ] `sql.PUBLIC_VIEWS` has no file containing `pubmed`. `views_sql()` and
      `restore_sql()` output contain no `pubmed_articles`.
- [ ] `publish_bundle.RAW_NAMESPACES` has no `"pubmed"`.
- [ ] `build_card` output links `cdsci-lake/issues/136`, says `2026-10` is the
      last release with `pubmed_articles.parquet`, and no longer lists PubMed as
      an included source or a redistributed provider.
- [ ] `test_marts_schema.py` passes unchanged (`pubmed_ids` / `pubmed_id`
      still present).
- [ ] The new tests in §4 exist and pass. `uv run pytest packages/omicidx-prefect/tests/`
      passes. `uv run ruff check` and `uv run ruff format --check` are clean on the
      changed files.
- [ ] `grep -rn pubmed_articles docs/*.qmd docs/overview docs/guides docs/_quarto.yml README.md`
      matches only the standard wording on `data-sources.qmd` (which names the
      `2026-10` release).
- [ ] `flows/pubmed.py`, `flows/ducklake_pubmed.py`, `flows/publish_release.py`,
      the SQLMesh pubmed models, and the systemd pubmed units are unchanged
      (`git diff --stat main` shows none of them).
- [ ] `CLAUDE.md` and ADR-0006 carry the notes in §6.
- [ ] The PR description lists the §7 operator step and says #236 stays open
      for phase 2 ("Refs #236", not "Closes #236").

## Test plan

1. `uv run pytest packages/omicidx-prefect/tests/` runs fully offline. The
   bundle tests serve local HTTP, and the hf_release tests are pure string
   building.
2. `views.sql`/`restore.sql` need no separate test: both iterate
   `PUBLIC_VIEWS`, which the new `test_bundle.py` test covers.
3. `quarto render docs` is optional locally. The docs edits are prose and table
   rows, with no executable cells touched.
4. After merge (maintainer): first nightly `omicidx-downstream` run succeeds.
   `curl -s https://omicidx.cancerdatasci.org/data/latest/manifest.json | jq '.tables[].table'`
   has no `pubmed_articles`. Then run the §7 deletion.

## Risks

- **Dropping a public table breaks users.** About 4 IPs read it in 90 days
  (cdsci-lake#136). Mitigation: HF `2026-10` keeps it permanently, and dated R2
  folders keep it for 30 days. The docs say so.
- **No public PubMed until cdsci-lake#136 ships.** The maintainer accepted this
  in the comment that started this phase. ADR-0006's local release keeps
  running, so nothing is lost.
- **Stale `latest/pubmed_articles.parquet`** if §7 is skipped: old `views.sql`
  copies would read a frozen file. Mitigation: §7 is in the PR description.
- **Wasted work:** the extract and lake load keep running for data that is not
  published. Accepted. It is the input cdsci-lake#136 ports, and phase 2 removes
  it.
