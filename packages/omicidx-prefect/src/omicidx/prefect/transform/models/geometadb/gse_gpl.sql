MODEL (
  name geometadb.gse_gpl,
  kind VIEW
);

SELECT DISTINCT
    accession AS gpl,
    UNNEST(series_id) AS gse
FROM src.geo_platforms;
