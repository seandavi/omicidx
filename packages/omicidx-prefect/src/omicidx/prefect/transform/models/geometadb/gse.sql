MODEL (
  name geometadb.gse,
  kind VIEW
);

WITH has_geo_computed_rnaseq AS (
    SELECT
        r.accession
    FROM
        src.geo_series_with_rnaseq_counts r
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
FROM src.geo_series g
LEFT JOIN has_geo_computed_rnaseq h
    ON g.accession = h.accession;
