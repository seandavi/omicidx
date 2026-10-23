MODEL (
  name stg.sra_samples,
  kind VIEW
);

-- The lake table, column for column, with organism renamed to current NCBI
-- Taxonomy by taxon_id (#226); unresolvable taxids keep the submitted name,
-- which rides along as organism_as_submitted.
SELECT
    s.* REPLACE (coalesce(n.scientific_name, s.organism) AS organism),
    s.organism AS organism_as_submitted
FROM src.sra_samples s
LEFT JOIN stg.ncbi_taxonomy_names n ON n.tax_id = s.taxon_id;
