-- description: SRA samples with organism resolved to the current NCBI Taxonomy name by taxon_id (#226); the submitted name is kept as organism_as_submitted.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
-- The lake table, column for column, with organism renamed to current NCBI
-- Taxonomy by taxon_id (#226); unresolvable taxids keep the submitted name,
-- which rides along as organism_as_submitted.
SELECT
    s.* REPLACE (coalesce(n.scientific_name, s.organism) AS organism),
    s.organism AS organism_as_submitted
FROM lake.src.sra_samples s
LEFT JOIN lake.stg.ncbi_taxonomy_names n ON n.tax_id = s.taxon_id
