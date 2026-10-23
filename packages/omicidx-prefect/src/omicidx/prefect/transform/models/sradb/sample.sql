MODEL (
  name sradb.sample,
  kind VIEW
);

SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS sample_ID,
    alias AS sample_alias,
    accession AS sample_accession,
    taxon_id,
    organism AS scientific_name,
    description,
    CAST(attributes AS VARCHAR) AS sample_attribute,
    biosample AS biosample_accession,
    organism_as_submitted AS scientific_name_as_submitted
FROM stg.sra_samples;
