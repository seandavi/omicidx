import { test } from "node:test";
import assert from "node:assert/strict";
import { permanentRedirect, route } from "../src/route.ts";

const at = (u: string) => route(new URL(u));

test("site host: docs everywhere but /data", () => {
  assert.deepEqual(at("https://omicidx.cancerdatasci.org/"), { kind: "docs" });
  assert.deepEqual(at("https://omicidx.cancerdatasci.org/guides/x/"), { kind: "docs" });
  assert.deepEqual(at("https://omicidx.cancerdatasci.org/database/"), { kind: "docs" });
});

test("site host: /data/* strips the prefix", () => {
  const data = (key: string) => ({ kind: "data", key, base: "/data" });
  assert.deepEqual(at("https://omicidx.cancerdatasci.org/data"), data(""));
  assert.deepEqual(at("https://omicidx.cancerdatasci.org/data/"), data(""));
  assert.deepEqual(
    at("https://omicidx.cancerdatasci.org/data/latest/a%20b.parquet"),
    data("latest/a b.parquet"),
  );
});

test("data host: bucket at /", () => {
  const data = (key: string) => ({ kind: "data", key, base: "" });
  assert.deepEqual(at("https://data.omicidx.cancerdatasci.org/"), data(""));
  assert.deepEqual(
    at("https://data.omicidx.cancerdatasci.org/latest/manifest.json"),
    data("latest/manifest.json"),
  );
  // no /data alias here: v{date}/data/ is a real bucket prefix
  assert.deepEqual(
    at("https://data.omicidx.cancerdatasci.org/data/x"),
    data("data/x"),
  );
});

test("legacy host (#211): basename for the /data/latest/ redirect", () => {
  const legacy = (name: string) => ({ kind: "legacy", name });
  assert.deepEqual(
    at("https://data-omicidx.cancerdatasci.org/duckdb/omicidx.duckdb"),
    legacy("omicidx.duckdb"),
  );
  assert.deepEqual(
    at("https://data-omicidx.cancerdatasci.org/sra/parquet/sra_runs.parquet"),
    legacy("sra_runs.parquet"),
  );
  assert.deepEqual(at("https://data-omicidx.cancerdatasci.org/"), legacy(""));
  assert.deepEqual(at("https://data-omicidx.cancerdatasci.org/geo/"), legacy(""));
});

test("permanentRedirect: assets' trailing-slash 307 becomes a 301 (#233)", () => {
  const r = permanentRedirect(new Response(null, { status: 307, headers: { location: "/guides/x" } }));
  assert.equal(r.status, 301);
  assert.equal(r.headers.get("location"), "/guides/x");
  const ok = new Response("hi");
  assert.equal(permanentRedirect(ok), ok);
});
