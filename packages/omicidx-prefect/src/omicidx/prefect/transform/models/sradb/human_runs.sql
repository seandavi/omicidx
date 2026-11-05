-- description: sradb.run_with_study restricted to human runs (taxon_id 9606).
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT * FROM lake.sradb.run_with_study WHERE taxon_id = 9606
