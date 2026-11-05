-- description: NCBI Taxonomy nodes with their scientific names: a thin view over lake.omicidx.ncbi_taxonomy, the single seam between the marts and the lake.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT * FROM lake.omicidx.ncbi_taxonomy
