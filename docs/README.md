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

Static container behind the existing Traefik proxy at `docs.omicidx.cancerdatasci.org`.

```bash
# On the server, from the omicidx repo root:
cd docs
docker compose up -d --build
```

The `Dockerfile` is multi-stage: `node:22-alpine` builds the static site, `caddy:2-alpine` serves it with zstd/gzip compression. The `proxy` Docker network and Cloudflare cert resolver are shared with the other services on this host.

To redeploy after content changes:

```bash
git pull
docker compose up -d --build
```

CI-driven rebuilds on push to `main` are not yet wired (manual `docker compose up -d --build` is the v1 workflow). See [#75](https://github.com/omicidx/omicidx/issues/75) for follow-ups.
