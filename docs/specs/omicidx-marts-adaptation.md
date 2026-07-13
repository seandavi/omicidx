# Marts adaptation & compatibility note (Endpoint 3, Stage C)

Fidelity bar (spec §(3)): **rough schema resemblance, modernized/adapted** —
recognizable legacy SRAdb/GEOmetadb table & column names, adapted where the
modern data warrants. Not a drop-in; not merely a query corpus.

This note is the C1 audit result and the C3 adaptation record: what maps, what
changed, what was dropped, what modern data is available but not yet surfaced.
Views live in `sql/040_geometadb_views.sql` (`geometadb.*`) and
`sql/050_sradb_views.sql` (`sradb.*`).

Legacy column lists below are from the Bioconductor SRAdb / GEOmetadb vignettes
(`study`, `gse`, `gpl` verbatim; `gsm` from `geodb_column_desc`; SRA
`sample`/`experiment`/`run`/`submission` from the bioruby-sra schema wiki). The
legacy denormalized `sra` table is undocumented in every vignette.

## Audit summary

- **No broken references.** Every column the mart views select resolves against
  the upstream `stg_*`/`src_*` views and the `omicidx.parsers` models.
- **No predecessor drift.** The prefect `040`/`050` files are byte-identical to
  the omicidx-etl and omicidx-dagster copies — C1 is pure modernization of an
  inherited legacy shell, not drift reconciliation.
- **SRA marts: 100% legacy-column coverage, name-for-name**, with heavy
  NULL-stubbing of columns absent from modern data.
- **GEO marts: strong coverage** plus a few genuinely-absent legacy curation
  columns.
- **Modernization applied:** the key modern cross-reference columns
  (GEO↔SRA↔BioProject, biosample, pubmed, library-layout stats) are now surfaced
  on the entity views — see Decisions taken.

## SRA (`sradb.*`)

| Legacy table | Legacy cols | Mapped | NULL-stubbed | Notes |
|---|---|---|---|---|
| `study` | 21 | 11 | 10 | `center_project_name` ← linked BioProject accession (adaptation). `study_attribute` ← `CAST(attributes AS VARCHAR)`. |
| `sample` | 20 | 8 | 12 | `scientific_name` ← `organism`. |
| `experiment` | 42 | 18 | 24 | `read_spec` ← `CAST(reads)`. `design_description` ← `design`. |
| `run` | 21 | 5 | 16 | `run_date`/`run_center` stubbed NULL **but present in `SraRun`** — see Deltas. |
| `sra` | (undocumented) | — | ~40 | Denormalized run⋈exp⋈sample⋈study; our reconstruction, `spots`/`bases` from run. |

NULL-stubbed everywhere and genuinely absent from modern data: `*_link`
(`sra_link`, `xref_link`, `*_entrez_link`, `study_url_link`, …), `ddbj_link`,
`ena_link`, `submission_*`, `sradb_updated`, `broker_name`, plus per-table
sequencing-detail columns (`base_caller`, `quality_scorer`, `platform_parameters`,
`adapter_spec`, `number_of_levels`, `multiplier`, `qtype`, `sequence_space`,
`bamFile`, `fastqFTP`, `total_data_blocks`, `run_file`, `sample_member`,
`targeted_loci`, `anonymized_name`, `individual_name`, `common_name`).

Still upstream but not surfaced (available if later wanted): `identifiers` /
`xrefs` on study/sample/experiment (arbitrary key-value structs, not clean
cross-refs). `sample.geo` and `experiment.library_layout_orientation` are *not*
selected by the ducklake loaders, so they are unavailable, not merely unmapped.

## GEO (`geometadb.*`)

| Legacy table | Legacy cols | Mapped | Dropped | Added (modern) |
|---|---|---|---|---|
| `gse` | 18 | 13 | `ID`, `repeats`, `repeats_sample_list`, `variable`, `variable_description` | exploded `contact_*`, `has_geo_computed_rnaseq` |
| `gsm` | 32 | 30 | `ID`, `gse` (link col) | `channel_records` (raw struct array) |
| `gpl` | 20 | 14 | `ID`, `coating`, `catalog_number`, `support`, `supplementary_file`, `bioc_package` | — |
| `gse_gsm` | 2 | 2 | — | — |
| `gse_gpl` | 2 | 2 | — | — |

Dropped legacy columns that are GDS/curation-era or Bioconductor-specific and
genuinely absent from the modern scrape: `gse.repeats*`/`variable*`,
`gpl.coating`/`catalog_number`/`support`/`bioc_package`. `gpl.supplementary_file`
has no field in `GEOPlatform`.

Modern addition with no legacy equivalent: `geometadb.geo_supplemental_files`
(exploded, `ftp://`→`https://`, `NONE` filtered).

Still upstream but not surfaced (available if later wanted): `gse` has
`sample_taxid`/`sample_organism`/`platform_taxid`/`platform_organism`/`relation`;
`sample_id`/`platform_id` already feed the `gse_gsm`/`gse_gpl` junctions.

## Decisions taken (2026-07-12)

1. **Modernize posture: add cross-refs as columns.** Modern link columns are
   surfaced directly on the entity views (not separate junctions):
   `study` → `bioproject_accession`, `geo_accession`, `pubmed_ids`;
   `sample`/`sra` → `biosample_accession`; `experiment` → `library_layout_length`,
   `library_layout_sdev`, `nreads`; `gse` → `bioprojects`, `sra_studies`,
   `subseries`; `gsm` → `biosample`, `sra_experiment`, `library_source`.
2. **Drop always-NULL columns.** The ~60 SRA columns hardcoded `NULL` (all
   `*_link`, `ddbj_link`, `ena_link`, `submission_*`, `sradb_updated`,
   `base_caller`, `bamFile`, …) are removed. Legacy queries naming them no longer
   parse — an accepted break, per the modernized (not drop-in) bar.
3. **Keep `supplemental_files`.** GEO views retain the current (plural) name; the
   legacy `supplementary_file` rename is documented here, not reverted.
4. **`run_date`/`run_center` dropped, not un-stubbed.** They are *not available*:
   the ducklake `sra_run` loader (`flows/ducklake_sra.py` `_RUN_SOURCE`) does not
   select them, so they never reach the lake table or parquet. Surfacing them is a
   data-layer change (add to `_RUN_SOURCE` + reload) tracked separately, not a
   mart edit.
5. **Surrogate `ID` columns** left as-is: SRA views keep `ROW_NUMBER()` `*_ID`;
   GEO views have none. Not stable keys either way; symmetry not pursued.

## Acceptance (C3)

`tests/test_marts_schema.py` builds the `geometadb.*`/`sradb.*` views over empty,
correctly-typed synthetic upstream tables (in-memory DuckDB, no catalog) and
asserts each view's exact output column set plus that dropped columns do not
reappear. End-to-end fidelity against the real parquet still needs a live-catalog
run (`ducklake-load → parquet-export → duckdb-build`).
