-- description: NCBI BioSample records with taxonomy_name resolved to the current NCBI Taxonomy name by taxon_id (#226); the submitted name is kept as taxonomy_name_as_submitted.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
-- The lake table, column for column, with taxonomy_name renamed to current
-- NCBI Taxonomy by taxon_id (#226); unresolvable taxids keep the submitted
-- name, which rides along as taxonomy_name_as_submitted.
SELECT
    b.* REPLACE (coalesce(n.scientific_name, b.taxonomy_name) AS taxonomy_name),
    b.taxonomy_name AS taxonomy_name_as_submitted
FROM lake.src.biosamples b
LEFT JOIN lake.stg.ncbi_taxonomy_names n ON n.tax_id = b.taxon_id
