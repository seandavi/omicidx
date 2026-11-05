-- description: GEOmetadb-compatible gpl table: one row per GEO platform.
-- license: upstream-ncbi-ebi-terms (https://www.ncbi.nlm.nih.gov/home/about/policies/)
-- materialized: view
SELECT
    title,
    accession AS gpl,
    status,
    submission_date,
    last_update_date,
    technology,
    distribution,
    organism,
    manufacturer,
    manufacture_protocol,
    description,
    'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' || accession AS web_link,
    contact."name"."first" || ' ' || contact."name"."last" AS contact,
    data_row_count,
    organism_as_submitted
FROM lake.stg.geo_platforms
