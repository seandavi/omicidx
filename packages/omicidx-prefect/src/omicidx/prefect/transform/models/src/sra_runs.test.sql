-- test: accession is unique
SELECT accession FROM lake.src.sra_runs GROUP BY accession HAVING count(*) > 1
