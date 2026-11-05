-- description: GEOmetadb-compatible gse table: one row per GEO series, plus has_geo_computed_rnaseq and modern cross-references.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
WITH has_geo_computed_rnaseq AS (
    SELECT
        r.accession
    FROM
        lake.src.geo_series_with_rnaseq_counts r
)
SELECT
    g.accession AS gse,
    title,
    status,
    submission_date,
    last_update_date,
    summary,
    pubmed_id,
    type,
    contributor,
    'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' || g.accession AS web_link,
    overall_design,
    contact.country AS contact_country,
    contact.email AS contact_email,
    contact."name"."first" AS contact_first_name,
    contact.institute AS contact_institute,
    contact."name"."last" AS contact_last_name,
    contact."name"."first" || ' ' || contact."name"."last" AS contact,
    supplemental_files,
    CASE WHEN h.accession IS NOT NULL THEN TRUE ELSE FALSE END AS has_geo_computed_rnaseq,
    bioprojects,
    sra_studies,
    subseries
FROM lake.src.geo_series g
LEFT JOIN has_geo_computed_rnaseq h
    ON g.accession = h.accession
