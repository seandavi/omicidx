MODEL (
  name stg.sra_studies,
  kind VIEW
);

SELECT
    accession,
    alias,
    title,
    description,
    abstract,
    study_type,
    center_name,
    broker_name,
    bioproject AS bioproject_accession,
    geo AS geo_accession,
    identifiers,
    attributes,
    xrefs,
    pubmed_ids
FROM src.sra_studies;
