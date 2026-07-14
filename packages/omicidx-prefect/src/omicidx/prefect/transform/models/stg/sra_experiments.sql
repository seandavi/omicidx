MODEL (
  name stg.sra_experiments,
  kind VIEW
);

SELECT
    accession,
    alias,
    title,
    design,
    center_name,
    study_accession,
    sample_accession,
    platform,
    instrument_model,
    library_name,
    library_construction_protocol,
    library_layout,
    TRY_CAST(library_layout_length AS INTEGER) AS library_layout_length,
    TRY_CAST(library_layout_sdev AS DOUBLE) AS library_layout_sdev,
    library_strategy,
    library_source,
    library_selection,
    spot_length,
    nreads,
    identifiers,
    attributes,
    xrefs,
    reads
FROM src.sra_experiments;
