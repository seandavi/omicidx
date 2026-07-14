MODEL (
  name geometadb.geo_supplemental_files,
  kind VIEW
);

WITH supp_file AS (
    SELECT
        accession,
        'gse' AS accession_type,
        UNNEST(supplemental_files) AS supplemental_file
    FROM src.geo_series

    UNION ALL

    SELECT
        accession,
        'gsm' AS accession_type,
        UNNEST(supplemental_files) AS supplemental_file
    FROM src.geo_samples
)
SELECT
    accession,
    accession_type,
    regexp_replace(supplemental_file, '^ftp://', 'https://') AS supplemental_file,
    regexp_extract(supplemental_file, '[^/]+$') AS filename
FROM supp_file
WHERE supplemental_file != 'NONE';
