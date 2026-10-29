MODEL (
  name geometadb.gse_gsm,
  kind VIEW
);

SELECT DISTINCT
    accession AS gse,
    UNNEST(sample_id) AS gsm
FROM src.geo_series
-- Sorted so the exported Parquet's row-group stats prune `WHERE gse = ...` (#233).
ORDER BY gse, gsm;
