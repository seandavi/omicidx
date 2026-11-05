-- description: SRA runs: a thin view over lake.omicidx.sra_run, the single seam between the marts and the lake.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT * FROM lake.omicidx.sra_run
