-- description: MetaHQ annotations with ontology term names: one row per (level, accession, attribute, source, value); term_name is NULL for sex/age values. To match a term and all its descendants (e.g. every brain sample), join metahq_ontology_ancestor on value = term_id.
-- license: CC-BY-NC-4.0 (MetaHQ, https://doi.org/10.5281/zenodo.17663086)
-- materialized: view
SELECT
    a.level,
    a.accession,
    a.attribute,
    a.source,
    a.ecode,
    a.value,
    t.name AS term_name,
    a.metahq_version
FROM lake.src.metahq_annotations AS a
LEFT JOIN lake.src.metahq_ontology_terms AS t ON a.value = t.term_id
-- Sorted so the exported Parquet's row-group stats prune `WHERE accession = ...`.
ORDER BY a.level, a.accession, a.attribute, a.source, a.value
