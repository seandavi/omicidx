/**
 * Where a request goes. The data host serves the bucket at `/` (published
 * bundle catalogs hard-code `https://data.omicidx.cancerdatasci.org/v{date}/data/`
 * as their data_path, so it must never redirect); every other host serves the
 * bucket under `/data/` and the docs everywhere else.
 */
export type Route =
  | { kind: "data"; key: string; base: string }
  | { kind: "docs" };

export function route(url: URL): Route {
  const path = decodeURIComponent(url.pathname);
  if (url.hostname.startsWith("data.")) {
    return { kind: "data", key: path.slice(1), base: "" };
  }
  if (path === "/data" || path.startsWith("/data/")) {
    return { kind: "data", key: path.slice("/data/".length), base: "/data" };
  }
  return { kind: "docs" };
}
