-- test: accession is unique
SELECT accession FROM lake.src.sra_studies GROUP BY accession HAVING count(*) > 1
