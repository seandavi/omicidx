# OmicIDX docs

[Astro](https://astro.build) + [Starlight](https://starlight.astro.build) static documentation site. Deploys to `https://docs.omicidx.cancerdatasci.org`.

## Local development

```bash
cd docs/
npm install        # one-time
npm run dev        # local dev server, hot reload at http://localhost:4321
npm run build      # static build into ./dist
npm run preview    # serve the production build locally
```

## Content layout

```
src/
├── components/                 # Reusable Astro components
│   └── Wip.astro               # 🚧 Construction-zone callout
└── content/
    └── docs/
        ├── index.mdx           # Landing page
        ├── overview/           # What OmicIDX is, sources, architecture
        └── guides/             # Getting the data, DuckDB, versions and citation
```

The sidebar TOC is configured in `astro.config.mjs`. The site is for people
using the data; contributor docs live in the repo (`CLAUDE.md`,
`systemd/README.md`, the package READMEs).

## Conventions

- **Markdown vs MDX:** plain `.md` for prose pages; `.mdx` only when the page imports a component (e.g., `Wip` or Starlight's `Card`).
- **Construction zones:** mark partially-written sections with the `Wip` component so visitors know what's stable. Out-of-date docs that claim authority are worse than no docs.

## Hosting

Served as Workers static assets by the one Cloudflare Worker in `site/`, at
https://omicidx.cancerdatasci.org (the same Worker serves the data bucket at
`/data/`). Its `[build]` step runs the Astro build, so to publish content
changes:

```bash
cd site && npx wrangler deploy
```

`docs.omicidx.cancerdatasci.org` and the retired `api-omicidx.cancerdatasci.org`
301 to the new host via a Traefik file-provider rule on onclappc02
(`monode/infrastructure/compose/traefik/config/omicidx-redirects.yml`).
