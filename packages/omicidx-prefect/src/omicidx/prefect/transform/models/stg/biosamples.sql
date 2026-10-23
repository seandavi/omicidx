MODEL (
  name stg.biosamples,
  kind VIEW
);

-- The lake table, column for column, with taxonomy_name renamed to current
-- NCBI Taxonomy by taxon_id (#226); unresolvable taxids keep the submitted
-- name, which rides along as taxonomy_name_as_submitted.
SELECT
    b.* REPLACE (coalesce(n.scientific_name, b.taxonomy_name) AS taxonomy_name),
    b.taxonomy_name AS taxonomy_name_as_submitted
FROM src.biosamples b
LEFT JOIN stg.ncbi_taxonomy_names n ON n.tax_id = b.taxon_id;
