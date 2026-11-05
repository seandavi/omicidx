-- description: sradb.run_with_study restricted to WGS library strategy.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT * FROM lake.sradb.run_with_study WHERE library_strategy = 'WGS'
