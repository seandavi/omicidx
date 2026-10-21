---
title: Versions and citation
description: latest vs dated vs monthly releases, how to pin a version, and how to cite OmicIDX.
---

OmicIDX publishes three kinds of versions. They hold the same tables in the
same layout; they differ in how long they last.

| Version | Where | Changes? | Kept |
|---|---|---|---|
| `latest/` | `https://omicidx.cancerdatasci.org/data/latest/` | rebuilt every night | always the newest |
| Dated | `https://omicidx.cancerdatasci.org/data/v{YYYY-MM-DD}/` | never | 30 days |
| Monthly release | HuggingFace dataset [`omicidx/omicidx`](https://huggingface.co/datasets/omicidx/omicidx), git tag `YYYY-MM` | never | long term, with a DOI |

## Which one to use

- **Exploring, or building something that should track NCBI:** use `latest/`.
- **An analysis you'll rerun in the next few weeks:** pin a dated folder.
- **A paper, a shared pipeline, or anything you'll cite:** use a monthly release.

## Pin a dated folder

Replace `latest/` with a date:

```sql
SELECT count(*)
FROM read_parquet('https://omicidx.cancerdatasci.org/data/v2026-10-20/sradb/study.parquet');
```

To see which dates are available, open the
[folder listing](https://omicidx.cancerdatasci.org/data/). Dated folders older
than 30 days are deleted. Use a monthly release for anything longer.

:::caution
In dated folders published before late October 2026, `omicidx.duckdb` and
`views.sql` point at `latest/`, not at their own folder. For those dates, pin
with `read_parquet` on the dated URLs, as above.
:::

## Use a monthly release

Once a month, one nightly build is copied to the HuggingFace dataset
`omicidx/omicidx` and tagged with its month (e.g. `2026-10`). Files sit at the
repository root in the same layout as R2 (`sra_runs.parquet`,
`sradb/study.parquet`, …). DuckDB reads them with `hf://` paths, and the `@tag`
pins the release:

```sql
SELECT count(*)
FROM 'hf://datasets/omicidx/omicidx@2026-10/sra_runs.parquet';
```

## Cite OmicIDX

Each monthly release gets a DOI, minted on HuggingFace. Cite the DOI of the
release you used, which you'll find on the dataset page for that tag. In your
methods, also state the release tag (e.g. `2026-10`).

If you used a dated folder instead, record its URL and the `transform_sha`
and `lake_snapshot_id` from its `manifest.json`. The folder will be gone in
30 days, so a monthly release is the better thing to cite.

## The manifest

Every version has a `manifest.json`:

```bash
curl -s https://omicidx.cancerdatasci.org/data/latest/manifest.json
```

```json
{
  "publish_date": "2026-10-20",
  "published_at": "2026-10-20T11:29:35.571633+00:00",
  "dated_path": "v2026-10-20/",
  "transform_sha": "90688e92966330aac9ecd5a4fb8dc1d126f7982f",
  "lake_snapshot_id": 4171,
  "raw_partition_counts": { "sra/study": 80, "geo": 260, "pubmed": 1689, … },
  "tables": [ { "schema": "sradb", "table": "study", "row_count": 774584 }, … ]
}
```

- `transform_sha` is the commit of the [pipeline code](https://github.com/seandavi/omicidx) that built the tables.
- `lake_snapshot_id` is the snapshot of the internal lake the tables were exported from.
- `tables` gives the row count of each published table.
