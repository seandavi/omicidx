MODEL (
  name stg.sra_runs,
  kind VIEW
);

SELECT
    r.accession,
    r.alias,
    r.experiment_accession,
    r.title,
    a.spots AS total_spots,
    a.bases AS total_bases,
    r.identifiers,
    r.attributes,
    r.qualities
FROM src.sra_runs r
LEFT JOIN src.sra_accessions a ON r.accession = a.accession;
