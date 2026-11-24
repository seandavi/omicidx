-- description: MetaHQ harmonized GEO annotations, one row per (level, accession, attribute, source, value): a thin view over lake.omicidx.metahq_annotation.
-- license: CC-BY-NC-4.0 (MetaHQ, https://doi.org/10.5281/zenodo.17663086)
-- materialized: view
SELECT * FROM lake.omicidx.metahq_annotation
