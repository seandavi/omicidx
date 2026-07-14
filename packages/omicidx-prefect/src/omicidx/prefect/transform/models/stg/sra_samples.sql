MODEL (
  name stg.sra_samples,
  kind VIEW
);

SELECT
    accession,
    alias,
    title,
    organism,
    description,
    taxon_id,
    biosample AS biosample_accession,
    identifiers,
    attributes,
    xrefs
FROM src.sra_samples;
