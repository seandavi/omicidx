-- description: PubMed articles, one row per pmid, with the published column set.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT DISTINCT ON (pmid)
    pmid,
    title,
    abstract,
    journal,
    medline_ta,
    country,
    issn_linking,
    nlm_unique_id,
    pubdate,
    date_completed,
    date_revised,
    doi,
    pmc,
    issue,
    pages,
    languages,
    vernacular_title,
    other_id,
    authors,
    mesh_terms,
    publication_types,
    chemical_list,
    keywords,
    "references",
    grant_ids
FROM lake.src.pubmed_articles
