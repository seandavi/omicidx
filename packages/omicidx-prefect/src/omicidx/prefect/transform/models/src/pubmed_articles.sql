MODEL (
  name src.pubmed_articles,
  kind VIEW
);

SELECT * FROM lake.omicidx.pubmed_article;
