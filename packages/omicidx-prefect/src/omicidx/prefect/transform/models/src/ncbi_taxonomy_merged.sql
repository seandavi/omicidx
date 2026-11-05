-- description: NCBI Taxonomy merged.dmp: old taxid to the taxid it merged into: a thin view over lake.omicidx.ncbi_taxonomy_merged, the single seam between the marts and the lake.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT * FROM lake.omicidx.ncbi_taxonomy_merged
