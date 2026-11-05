-- description: GEO series that have NCBI-computed RNA-seq counts: a thin view over lake.omicidx.geo_series_with_rnaseq_counts, the single seam between the marts and the lake.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT * FROM lake.omicidx.geo_series_with_rnaseq_counts
