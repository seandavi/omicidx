MODEL (
  name stg.geo_series,
  kind VIEW
);

-- The lake table, column for column, with sample_organism/platform_organism
-- renamed to current NCBI Taxonomy by the element-aligned *_taxid lists (#226;
-- GEO renames without bumping last_update_date). Unresolvable taxids keep the
-- submitted name; the submitted lists ride along as *_as_submitted.
WITH organisms AS (
    SELECT
        accession,
        'sample' AS kind,
        unnest(sample_taxid) AS taxid,
        unnest(sample_organism) AS organism,
        generate_subscripts(sample_taxid, 1) AS i
    FROM src.geo_series
    WHERE len(sample_taxid) = len(sample_organism)
    UNION ALL
    SELECT
        accession,
        'platform',
        unnest(platform_taxid),
        unnest(platform_organism),
        generate_subscripts(platform_taxid, 1)
    FROM src.geo_series
    WHERE len(platform_taxid) = len(platform_organism)
),
resolved AS (
    SELECT
        o.accession,
        list(coalesce(n.scientific_name, o.organism) ORDER BY o.i)
            FILTER (o.kind = 'sample') AS sample_organism,
        list(coalesce(n.scientific_name, o.organism) ORDER BY o.i)
            FILTER (o.kind = 'platform') AS platform_organism
    FROM organisms o
    LEFT JOIN stg.ncbi_taxonomy_names n ON n.tax_id = o.taxid
    GROUP BY o.accession
)
SELECT
    g.* REPLACE (
        coalesce(r.sample_organism, g.sample_organism) AS sample_organism,
        coalesce(r.platform_organism, g.platform_organism) AS platform_organism
    ),
    g.sample_organism AS sample_organism_as_submitted,
    g.platform_organism AS platform_organism_as_submitted
FROM src.geo_series g
LEFT JOIN resolved r ON r.accession = g.accession;
