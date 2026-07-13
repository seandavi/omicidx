-- SRAdb-compatible views over OmicIDX parquet files
-- These views approximate the original SRAmetadb.sqlite schema, MODERNIZED:
-- legacy column names are kept where the data exists, columns that were always
-- NULL in the modern scrape are dropped, and modern cross-reference columns
-- (bioproject/geo/biosample links, library layout stats) are added.
-- See docs/specs/omicidx-marts-adaptation.md for the full mapping.
--
-- Usage:
--   1. First run 020_base_parquet_views.sql to create the src_* views
--   2. Then run 030_staging_views.sql to create the stg_* views (deduplicated)
--   3. Then run this file to create the sradb schema and views

CREATE SCHEMA IF NOT EXISTS sradb;

USE sradb;

-----
-- study table  (stg_sra_studies -> sradb.study)
-----
CREATE OR REPLACE VIEW study AS
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS study_ID,
    alias AS study_alias,
    accession AS study_accession,
    title AS study_title,
    study_type,
    abstract AS study_abstract,
    broker_name,
    center_name,
    bioproject_accession AS center_project_name,
    description AS study_description,
    CAST(attributes AS VARCHAR) AS study_attribute,
    -- modern cross-references
    bioproject_accession,
    geo_accession,
    pubmed_ids
FROM main.stg_sra_studies;

-----
-- sample table  (stg_sra_samples -> sradb.sample)
-----
CREATE OR REPLACE VIEW sample AS
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS sample_ID,
    alias AS sample_alias,
    accession AS sample_accession,
    taxon_id,
    organism AS scientific_name,
    description,
    CAST(attributes AS VARCHAR) AS sample_attribute,
    -- modern cross-references
    biosample_accession
FROM main.stg_sra_samples;

-----
-- experiment table  (stg_sra_experiments -> sradb.experiment)
-----
CREATE OR REPLACE VIEW experiment AS
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
    -- modern columns
    library_layout_length,
    library_layout_sdev,
    nreads
FROM main.stg_sra_experiments;

-----
-- run table  (stg_sra_runs -> sradb.run)
-- run_date/run_center are absent: the ducklake sra_run loader does not carry
-- them (docs/specs/omicidx-marts-adaptation.md §Deltas).
-----
CREATE OR REPLACE VIEW run AS
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS run_ID,
    alias AS run_alias,
    accession AS run_accession,
    experiment_accession,
    total_spots,
    total_bases,
    CAST(attributes AS VARCHAR) AS run_attribute
FROM main.stg_sra_runs;

-----
-- sra table (denormalized join of all entities)
-- The main table SRAdb users query. Modern reconstruction: legacy `sra` is
-- undocumented, so this is the union of the normalized views above, keyed on
-- the *_accession columns, with always-NULL legacy columns dropped and modern
-- cross-references retained.
-----
CREATE OR REPLACE VIEW sra AS
SELECT
    ROW_NUMBER() OVER (ORDER BY r.accession) AS sra_ID,
    -- Run fields
    r.alias AS run_alias,
    r.accession AS run_accession,
    r.total_spots AS spots,
    r.total_bases AS bases,
    CAST(r.attributes AS VARCHAR) AS run_attribute,
    -- Experiment fields
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
    -- Sample fields
    sa.alias AS sample_alias,
    sa.accession AS sample_accession,
    sa.taxon_id,
    sa.organism AS scientific_name,
    sa.description,
    CAST(sa.attributes AS VARCHAR) AS sample_attribute,
    sa.biosample_accession,
    -- Study fields
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
FROM main.stg_sra_runs r
LEFT JOIN main.stg_sra_experiments e ON r.experiment_accession = e.accession
LEFT JOIN main.stg_sra_samples sa ON e.sample_accession = sa.accession
LEFT JOIN main.stg_sra_studies st ON e.study_accession = st.accession;

-----
-- Convenience views for common queries
-----

-- Run info with study context (common use case)
CREATE OR REPLACE VIEW run_with_study AS
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
FROM main.stg_sra_runs r
LEFT JOIN main.stg_sra_experiments e ON r.experiment_accession = e.accession
LEFT JOIN main.stg_sra_samples sa ON e.sample_accession = sa.accession
LEFT JOIN main.stg_sra_studies st ON e.study_accession = st.accession;

-- RNA-seq experiments (common filter)
CREATE OR REPLACE VIEW rnaseq_runs AS
SELECT * FROM run_with_study WHERE library_strategy = 'RNA-Seq';

-- WGS experiments
CREATE OR REPLACE VIEW wgs_runs AS
SELECT * FROM run_with_study WHERE library_strategy = 'WGS';

-- Human samples
CREATE OR REPLACE VIEW human_runs AS
SELECT * FROM run_with_study WHERE taxon_id = 9606;

-- Mouse samples
CREATE OR REPLACE VIEW mouse_runs AS
SELECT * FROM run_with_study WHERE taxon_id = 10090;

-- End of SRAdb-compatible views
use main;
