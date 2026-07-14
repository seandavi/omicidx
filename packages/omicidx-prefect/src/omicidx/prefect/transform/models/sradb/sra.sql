MODEL (
  name sradb.sra,
  kind VIEW
);

SELECT
    ROW_NUMBER() OVER (ORDER BY r.accession) AS sra_ID,
    r.alias AS run_alias,
    r.accession AS run_accession,
    r.total_spots AS spots,
    r.total_bases AS bases,
    CAST(r.attributes AS VARCHAR) AS run_attribute,
    e.alias AS experiment_alias,
    e.accession AS experiment_accession,
    e.title AS experiment_title,
    e.design AS design_description,
    e.library_name,
    e.library_strategy,
    e.library_source,
    e.library_selection,
    e.library_layout,
    e.library_construction_protocol,
    CAST(e.reads AS VARCHAR) AS read_spec,
    e.platform,
    e.instrument_model,
    CAST(e.attributes AS VARCHAR) AS experiment_attribute,
    sa.alias AS sample_alias,
    sa.accession AS sample_accession,
    sa.taxon_id,
    sa.organism AS scientific_name,
    sa.description,
    CAST(sa.attributes AS VARCHAR) AS sample_attribute,
    sa.biosample_accession,
    st.alias AS study_alias,
    st.accession AS study_accession,
    st.title AS study_title,
    st.study_type,
    st.abstract AS study_abstract,
    st.bioproject_accession AS center_project_name,
    st.bioproject_accession,
    st.geo_accession,
    st.description AS study_description,
    CAST(st.attributes AS VARCHAR) AS study_attribute
FROM stg.sra_runs r
LEFT JOIN stg.sra_experiments e ON r.experiment_accession = e.accession
LEFT JOIN stg.sra_samples sa ON e.sample_accession = sa.accession
LEFT JOIN stg.sra_studies st ON e.study_accession = st.accession;
