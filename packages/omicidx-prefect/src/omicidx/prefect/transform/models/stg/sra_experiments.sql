-- description: SRA experiments with library_layout_length/library_layout_sdev typed as INTEGER/DOUBLE.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
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
FROM lake.src.sra_experiments
