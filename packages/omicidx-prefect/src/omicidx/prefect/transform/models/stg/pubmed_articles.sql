MODEL (
  name stg.pubmed_articles,
  kind VIEW
);

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
FROM src.pubmed_articles;
