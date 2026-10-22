// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  site: "https://omicidx.cancerdatasci.org",
  integrations: [
    starlight({
      title: "OmicIDX",
      // The one GA4 property shared by every site (G-KLLV1GCF4E).
      head: [
        {
          tag: "script",
          attrs: {
            async: true,
            src: "https://www.googletagmanager.com/gtag/js?id=G-KLLV1GCF4E",
          },
        },
        {
          tag: "script",
          content:
            "window.dataLayer=window.dataLayer||[];function gtag(){dataLayer.push(arguments);}gtag('js',new Date());gtag('config','G-KLLV1GCF4E');",
        },
      ],
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
            { label: "Use from R (omicidxr)", slug: "guides/r-omicidxr" },
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
