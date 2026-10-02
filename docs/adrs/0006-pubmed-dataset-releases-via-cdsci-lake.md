# 0006. PubMed dataset releases via cdsci-lake

- **Status**: Accepted
- Date: 2026-10-02

## Context

omicidx serves PubMed from the shared DuckLake table `lake.omicidx.pubmed_article`
and publishes its bundle through `parquet_export`/`publish_bundle`. ADR-0004 keeps
"past snapshots never deleted". The platform has moved to versioned datasets, not
versioned rows (cdsci-lake ADR-0025): each dataset publishes immutable full-snapshot
releases with a release id (`YYYY-MM-DD[.N]`) and its own retention rule.

## Decision

`omicidx-pubmed` is published through `cdsci.lake.publish`
(`publish_release`, flow `omicidx.prefect.flows.publish_release`), daily, as a full
snapshot of `lake.omicidx.pubmed_article` (one row per PMID, latest MEDLINE revision,
deleted citations absent). The dataset declares `keep_last=7`: only the newest seven
releases are kept. Pinned releases (`cdsci.lake.publish.index.pin_release`) are never
pruned. Output is local storage only; no bucket or R2 adapter is part of this decision.

This amends ADR-0004's "past snapshots never deleted" for this dataset only. The
existing bundle (`parquet_export`/`publish_bundle`) is untouched; moving it onto
`cdsci.lake.publish` is follow-on work.

## Consequences

- Reproducibility beyond seven days requires pinning a release.
- Upstream PubMed update-file diffs remain the only row-level history.
