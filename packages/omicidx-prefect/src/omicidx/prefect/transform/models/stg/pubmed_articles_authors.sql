-- description: PubMed article authors unnested, one row per (pmid, ordinal) in author-list order; the nested authors column stays on lake.stg.pubmed_articles (#264). Lake only, not in the omicidx bundle (#236).
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
-- (pmid, ordinal) is unique by construction: WITH ORDINALITY numbers the
-- elements within each parent row and src/pubmed_articles.test.sql asserts the
-- parent key is unique. No .test.sql here on purpose: a GROUP BY over this
-- view costs minutes per daily run (#264, measured in the PR).
SELECT
    p.pmid,
    a.ordinal,
    a.author.lastname,
    a.author.forename,
    a.author.initials,
    a.author.identifier,
    a.author.affiliation
FROM lake.src.pubmed_articles p,
    unnest(p.authors) WITH ORDINALITY AS a(author, ordinal)
