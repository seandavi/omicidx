-- description: SRA studies with bioproject/geo renamed to bioproject_accession/geo_accession.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
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
FROM lake.src.sra_studies
