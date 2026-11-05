-- description: SRA sample attributes unnested, one row per (accession, ordinal) in source order; the nested attributes column stays on lake.stg.sra_samples (#264).
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
-- (accession, ordinal) is unique by construction: WITH ORDINALITY numbers the
-- elements within each parent row and src/sra_samples.test.sql asserts the
-- parent key is unique. No .test.sql here on purpose: a GROUP BY over this
-- view costs minutes per daily run (#264, measured in the PR).
SELECT
    p.accession,
    a.ordinal,
    a.attribute.tag,
    a.attribute.value
FROM lake.src.sra_samples p,
    unnest(p.attributes) WITH ORDINALITY AS a(attribute, ordinal)
