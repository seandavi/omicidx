-- description: PubMed article grants unnested, one row per (pmid, ordinal) in source order; the nested grant_ids column stays on lake.stg.pubmed_articles (#264). Lake only, not in the omicidx bundle (#236).
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
-- (pmid, ordinal) is unique by construction: WITH ORDINALITY numbers the
-- elements within each parent row and src/pubmed_articles.test.sql asserts the
-- parent key is unique. No .test.sql here on purpose: a GROUP BY over this
-- view costs minutes per daily run (#264, measured in the PR).
SELECT
    p.pmid,
    g.ordinal,
    g.grant_rec.grant_id,
    g.grant_rec.grant_acronym,
    g.grant_rec.country,
    g.grant_rec.agency
FROM lake.src.pubmed_articles p,
    unnest(p.grant_ids) WITH ORDINALITY AS g(grant_rec, ordinal)
