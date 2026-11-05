-- description: GEOmetadb-compatible gse_gpl link table: series to platform.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT DISTINCT
    accession AS gpl,
    UNNEST(series_id) AS gse
FROM lake.src.geo_platforms
