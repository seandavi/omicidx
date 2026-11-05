-- test: accession is unique
SELECT accession FROM lake.src.sra_experiments GROUP BY accession HAVING count(*) > 1
