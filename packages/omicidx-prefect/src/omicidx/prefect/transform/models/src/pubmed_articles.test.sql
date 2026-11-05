-- test: pmid is unique
SELECT pmid FROM lake.src.pubmed_articles GROUP BY pmid HAVING count(*) > 1
