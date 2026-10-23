import { test } from "node:test";
import assert from "node:assert/strict";
import { conditionalHeaders } from "../src/conditional.ts";

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
