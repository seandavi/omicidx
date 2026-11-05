-- test: accession is unique
SELECT accession FROM lake.src.biosamples GROUP BY accession HAVING count(*) > 1
