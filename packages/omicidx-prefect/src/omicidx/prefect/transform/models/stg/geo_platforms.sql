MODEL (
  name stg.geo_platforms,
  kind VIEW
);

-- The lake table, column for column, with organism renamed to current NCBI
-- Taxonomy by taxid (#231; GEO renames without bumping last_update_date). A
-- platform's organism is its organisms newline-joined, aligned with its taxid
-- list; unresolvable taxids, NULL taxid (rows not yet re-extracted) and
-- misaligned lists keep the submitted name, which rides along as
-- organism_as_submitted.
WITH platform_taxa AS (
    SELECT
        accession,
        unnest(taxid) AS taxid,
        unnest(string_split(organism, chr(10))) AS organism,
        generate_subscripts(taxid, 1) AS i
    FROM src.geo_platforms
    WHERE len(taxid) = len(string_split(organism, chr(10)))
),
resolved AS (
    SELECT
        t.accession,
        string_agg(coalesce(n.scientific_name, t.organism), chr(10) ORDER BY t.i)
            AS organism
    FROM platform_taxa t
    LEFT JOIN stg.ncbi_taxonomy_names n ON n.tax_id = t.taxid
    GROUP BY t.accession
)
SELECT
    g.* REPLACE (coalesce(r.organism, g.organism) AS organism),
    g.organism AS organism_as_submitted
FROM src.geo_platforms g
LEFT JOIN resolved r ON r.accession = g.accession;
