-- description: Current scientific name for every taxid a record may cite, merged taxids included (#226); one row per tax_id.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
-- Current scientific name for any taxid a record may cite (#226): live taxa by
-- their own id, merged-away taxa by the taxon they were merged into. One row
-- per tax_id, so joining on it never fans a record out.
SELECT tax_id, scientific_name
FROM (
    SELECT tax_id, scientific_name, 0 AS merged
    FROM lake.src.ncbi_taxonomy
    UNION ALL
    SELECT m.old_tax_id, t.scientific_name, 1
    FROM lake.src.ncbi_taxonomy_merged m
    JOIN lake.src.ncbi_taxonomy t ON t.tax_id = m.new_tax_id
)
QUALIFY row_number() OVER (PARTITION BY tax_id ORDER BY merged) = 1
