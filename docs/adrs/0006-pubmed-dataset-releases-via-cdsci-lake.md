# 0006. PubMed dataset releases via cdsci-lake

- **Status**: Superseded (2026-11-04) by cdsci-lake
  [ADR-0027](https://github.com/seandavi/cdsci-lake/blob/main/docs/adr/0027-shared-release-bundle-package.md)
  and cdsci-lake#136
- Date: 2026-10-02

> **2026-11-04.** `flows/publish_release.py` is removed. It was never
> scheduled (no systemd unit, not in the downstream chain or the CLI) and wrote
> to a temporary directory by default, and `cdsci.lake.publish`, the module it
> was built on, is retired in favour of `cdsci.publish` (ADR-0027). omicidx no
> longer imports `cdsci.lake.publish`. The `omicidx-pubmed` column contract
> (`PUBMED_ARTICLE` / `PUBMED`, for cdsci-lake#136 to port) last lives at commit
> [`8b6e123`](https://github.com/seandavi/omicidx/blob/8b6e12367e5659ac1b00fed3bee325ecf99f05b7/packages/omicidx-prefect/src/omicidx/prefect/flows/publish_release.py).
> The pubmed extract and lake load stay until cdsci-lake#136 ships (#236).

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
- 2026-10-27: `pubmed_articles` left the OmicIDX public bundle (#236). This
  release is now the only copy of PubMed that omicidx produces, until
  cdsci-lake#136 takes it over.
