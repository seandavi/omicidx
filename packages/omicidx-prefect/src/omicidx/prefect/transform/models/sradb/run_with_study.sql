-- description: One row per SRA run, denormalized with its experiment, sample and study.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT
    r.accession AS run_accession,
    r.total_spots,
    r.total_bases,
    e.accession AS experiment_accession,
    e.library_strategy,
    e.library_source,
    e.library_selection,
    e.library_layout,
    e.platform,
    e.instrument_model,
    sa.accession AS sample_accession,
    sa.organism,
    sa.taxon_id,
    st.accession AS study_accession,
    st.title AS study_title,
    st.study_type,
    st.bioproject_accession AS bioproject,
    sa.organism_as_submitted
FROM lake.stg.sra_runs r
LEFT JOIN lake.stg.sra_experiments e ON r.experiment_accession = e.accession
LEFT JOIN lake.stg.sra_samples sa ON e.sample_accession = sa.accession
LEFT JOIN lake.stg.sra_studies st ON e.study_accession = st.accession
