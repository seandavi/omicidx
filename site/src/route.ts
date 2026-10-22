/**
 * Where a request goes. The data host serves the bucket at `/` (published
 * bundle catalogs hard-code `https://data.omicidx.cancerdatasci.org/v{date}/data/`
 * as their data_path, so it must never redirect); every other host serves the
 * bucket under `/data/` and the docs everywhere else.
 *
 * The retired host `data-omicidx.cancerdatasci.org` (#211) only redirects:
 * `name` is the requested file's basename, sent to `/data/latest/{name}` when
 * that object exists, else to the docs home.
 */
export type Route =
  | { kind: "data"; key: string; base: string }
  | { kind: "legacy"; name: string }
  | { kind: "docs" };

export function route(url: URL): Route {
  const path = decodeURIComponent(url.pathname);
  if (url.hostname === "data-omicidx.cancerdatasci.org") {
    return { kind: "legacy", name: path.slice(path.lastIndexOf("/") + 1) };
  }
  if (url.hostname.startsWith("data.")) {
    return { kind: "data", key: path.slice(1), base: "" };
  }
  if (path === "/data" || path.startsWith("/data/")) {
    return { kind: "data", key: path.slice("/data/".length), base: "/data" };
  }
  return { kind: "docs" };
}
