MODEL (
  name sradb.experiment,
  kind VIEW
);

SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS experiment_ID,
    alias AS experiment_alias,
    accession AS experiment_accession,
    center_name,
    title,
    study_accession,
    design AS design_description,
    sample_accession,
    library_name,
    library_strategy,
    library_source,
    library_selection,
    library_layout,
    library_construction_protocol,
    spot_length,
    CAST(reads AS VARCHAR) AS read_spec,
    platform,
    instrument_model,
    CAST(attributes AS VARCHAR) AS experiment_attribute,
    library_layout_length,
    library_layout_sdev,
    nreads
FROM stg.sra_experiments;
