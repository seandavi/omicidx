-- description: sradb.run_with_study restricted to mouse runs (taxon_id 10090).
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT * FROM lake.sradb.run_with_study WHERE taxon_id = 10090
