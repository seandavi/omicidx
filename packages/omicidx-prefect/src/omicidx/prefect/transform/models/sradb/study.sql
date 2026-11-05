-- description: SRAdb-compatible study table: one row per SRA study.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS study_id,
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
FROM lake.stg.sra_studies
