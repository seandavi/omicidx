-- description: GEOmetadb-compatible gse_gsm link table: series to sample, sorted for row-group pruning (#233).
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT DISTINCT
    accession AS gse,
    UNNEST(sample_id) AS gsm
FROM lake.src.geo_series
-- Sorted so the exported Parquet's row-group stats prune `WHERE gse = ...` (#233).
ORDER BY gse, gsm
