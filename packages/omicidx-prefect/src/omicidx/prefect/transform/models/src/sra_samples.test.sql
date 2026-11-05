-- test: accession is unique
SELECT accession FROM lake.src.sra_samples GROUP BY accession HAVING count(*) > 1
