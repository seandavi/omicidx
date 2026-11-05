-- description: NCBI BioSample attributes unnested, one row per (accession, ordinal) in source order; the nested attribute_recs column stays on lake.stg.biosamples (#264).
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
-- (accession, ordinal) is unique by construction: WITH ORDINALITY numbers the
-- elements within each parent row and src/biosamples.test.sql asserts the
-- parent key is unique. No .test.sql here on purpose: a GROUP BY over this
-- view costs minutes per daily run (#264, measured in the PR).
SELECT
    p.accession,
    a.ordinal,
    a.attribute.attribute_name,
    a.attribute.value,
    a.attribute.harmonized_name,
    a.attribute.display_name,
    a.attribute.unit
FROM lake.src.biosamples p,
    unnest(p.attribute_recs) WITH ORDINALITY AS a(attribute, ordinal)
