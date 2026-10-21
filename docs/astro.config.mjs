// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  site: "https://omicidx.cancerdatasci.org",
  integrations: [
    starlight({
      title: "OmicIDX",
      description:
        "Public, queryable index of NCBI SRA, GEO, BioSample, BioProject, and PubMed metadata.",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/seandavi/omicidx",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/seandavi/omicidx/edit/main/docs/",
      },
      lastUpdated: true,
      sidebar: [
        {
          label: "Overview",
          items: [
            { label: "What is OmicIDX?", slug: "overview/what-is-omicidx" },
            { label: "Data sources", slug: "overview/data-sources" },
            { label: "Architecture", slug: "overview/architecture" },
          ],
        },
        {
          label: "Guides",
          items: [
            { label: "Get the data", slug: "guides" },
            { label: "Query with DuckDB", slug: "guides/duckdb-snapshot" },
            {
              label: "Versions and citation",
              slug: "guides/versions-and-citation",
            },
          ],
        },
      ],
    }),
  ],
});
