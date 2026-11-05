-- description: GEOmetadb-compatible: one row per GSE/GSM supplemental file, ftp:// rewritten to https://.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
WITH supp_file AS (
    SELECT
        accession,
        'gse' AS accession_type,
        UNNEST(supplemental_files) AS supplemental_file
    FROM lake.src.geo_series

    UNION ALL

    SELECT
        accession,
        'gsm' AS accession_type,
        UNNEST(supplemental_files) AS supplemental_file
    FROM lake.src.geo_samples
)
SELECT
    accession,
    accession_type,
    regexp_replace(supplemental_file, '^ftp://', 'https://') AS supplemental_file,
    regexp_extract(supplemental_file, '[^/]+$') AS filename
FROM supp_file
WHERE supplemental_file != 'NONE'
