import { test } from "node:test";
import assert from "node:assert/strict";
import { conditionalHeaders, failedConditionStatus } from "../src/conditional.ts";

test("conditionalHeaders decodes DuckDB-Wasm's percent-encoded ETag quotes (#227)", () => {
  const h = conditionalHeaders(new Headers({
    "if-match": "%22d3f0%22",
    "if-none-match": '"abc"',
    range: "bytes=0-12287",
  }));
  assert.equal(h.get("if-match"), '"d3f0"');
  assert.equal(h.get("if-none-match"), '"abc"');
  assert.equal(h.get("range"), "bytes=0-12287");
});

test("failedConditionStatus: a failed If-Match is 412, If-None-Match is 304 (#233)", () => {
  const status = (h: Record<string, string>) =>
    failedConditionStatus(new Headers(h), '"abc"', new Date("2026-10-24T03:00:00.500Z"));
  assert.equal(status({ "if-match": '"stale"' }), 412);
  assert.equal(status({ "if-match": '"stale", "abc"', "if-none-match": '"abc"' }), 304);
  assert.equal(status({ "if-none-match": '"abc"' }), 304);
  assert.equal(status({ "if-unmodified-since": "Fri, 23 Oct 2026 00:00:00 GMT" }), 412);
  assert.equal(status({ "if-unmodified-since": "Sat, 24 Oct 2026 03:00:00 GMT" }), 304);
  assert.equal(status({ "if-modified-since": "Sat, 24 Oct 2026 03:00:00 GMT" }), 304);
});
