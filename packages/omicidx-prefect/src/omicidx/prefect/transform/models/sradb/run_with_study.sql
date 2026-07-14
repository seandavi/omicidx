MODEL (
  name sradb.run_with_study,
  kind VIEW
);

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
    st.bioproject_accession AS BioProject
FROM stg.sra_runs r
LEFT JOIN stg.sra_experiments e ON r.experiment_accession = e.accession
LEFT JOIN stg.sra_samples sa ON e.sample_accession = sa.accession
LEFT JOIN stg.sra_studies st ON e.study_accession = st.accession;
