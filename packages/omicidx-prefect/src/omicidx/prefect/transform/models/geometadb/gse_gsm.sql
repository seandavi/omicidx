MODEL (
  name geometadb.gse_gsm,
  kind VIEW
);

SELECT DISTINCT
    accession AS gse,
    UNNEST(sample_id) AS gsm
FROM src.geo_series;
