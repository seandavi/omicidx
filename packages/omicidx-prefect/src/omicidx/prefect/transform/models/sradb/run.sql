MODEL (
  name sradb.run,
  kind VIEW
);

SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS run_ID,
    alias AS run_alias,
    accession AS run_accession,
    experiment_accession,
    total_spots,
    total_bases,
    CAST(attributes AS VARCHAR) AS run_attribute
FROM stg.sra_runs;
