-- description: SRAdb-compatible sample table: one row per SRA sample.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS sample_id,
    alias AS sample_alias,
    accession AS sample_accession,
    taxon_id,
    organism AS scientific_name,
    description,
    CAST(attributes AS VARCHAR) AS sample_attribute,
    biosample AS biosample_accession,
    organism_as_submitted AS scientific_name_as_submitted
FROM lake.stg.sra_samples
