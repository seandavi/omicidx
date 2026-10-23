MODEL (
  name stg.geo_samples,
  kind VIEW
);

-- The lake table, column for column, with channels[].organism renamed to
-- current NCBI Taxonomy by channels[].taxid (#226). A channel's organism is
-- its organisms newline-joined, aligned with its taxid list; unresolvable
-- taxids keep the submitted name. The channels struct type is unchanged; the
-- submitted per-channel names ride along as channel_organism_as_submitted.
WITH channels AS (
    SELECT accession, ci, c.taxid AS taxids, c.organism
    FROM (
        SELECT
            accession,
            unnest(channels) AS c,
            generate_subscripts(channels, 1) AS ci
        FROM src.geo_samples
    )
),
channel_taxa AS (
    SELECT
        accession,
        ci,
        unnest(taxids) AS taxid,
        unnest(string_split(organism, chr(10))) AS organism,
        generate_subscripts(taxids, 1) AS ti
    FROM channels
    WHERE len(taxids) = len(string_split(organism, chr(10)))
),
channel_names AS (
    SELECT
        t.accession,
        t.ci,
        string_agg(coalesce(n.scientific_name, t.organism), chr(10) ORDER BY t.ti)
            AS organism
    FROM channel_taxa t
    LEFT JOIN stg.ncbi_taxonomy_names n ON n.tax_id = t.taxid
    GROUP BY t.accession, t.ci
),
resolved AS (
    SELECT
        c.accession,
        list(coalesce(cn.organism, c.organism) ORDER BY c.ci) AS channel_organism
    FROM channels c
    LEFT JOIN channel_names cn ON cn.accession = c.accession AND cn.ci = c.ci
    GROUP BY c.accession
)
SELECT
    g.* REPLACE (
        list_transform(
            g.channels,
            lambda ch, i: struct_update(ch, organism := r.channel_organism[i])
        ) AS channels
    ),
    list_transform(g.channels, lambda ch: ch.organism) AS channel_organism_as_submitted
FROM src.geo_samples g
LEFT JOIN resolved r ON r.accession = g.accession;
