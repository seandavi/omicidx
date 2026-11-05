-- description: SRA runs with total_spots/total_bases joined in from SRA_Accessions.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
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
FROM lake.src.sra_runs r
LEFT JOIN lake.src.sra_accessions a ON r.accession = a.accession
