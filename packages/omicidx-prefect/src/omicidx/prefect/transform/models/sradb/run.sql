-- description: SRAdb-compatible run table: one row per SRA run.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS run_id,
    alias AS run_alias,
    accession AS run_accession,
    experiment_accession,
    total_spots,
    total_bases,
    CAST(attributes AS VARCHAR) AS run_attribute
FROM lake.stg.sra_runs
