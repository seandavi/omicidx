MODEL (
  name sradb.study,
  kind VIEW
);

SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS study_ID,
    alias AS study_alias,
    accession AS study_accession,
    title AS study_title,
    study_type,
    abstract AS study_abstract,
    broker_name,
    center_name,
    bioproject_accession AS center_project_name,
    description AS study_description,
    CAST(attributes AS VARCHAR) AS study_attribute,
    bioproject_accession,
    geo_accession,
    pubmed_ids
FROM stg.sra_studies;
