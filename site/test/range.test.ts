import { test } from "node:test";
import assert from "node:assert/strict";
import { parseRange } from "../src/range.ts";

test("parseRange", () => {
  assert.deepEqual(parseRange("bytes=0-99", 1000), { offset: 0, length: 100 });
  assert.deepEqual(parseRange("bytes=900-", 1000), { offset: 900, length: 100 });
  assert.deepEqual(parseRange("bytes=990-5000", 1000), { offset: 990, length: 10 });
  assert.deepEqual(parseRange("bytes=-8", 1000), { offset: 992, length: 8 });
  assert.deepEqual(parseRange("bytes=-5000", 1000), { offset: 0, length: 1000 });
  assert.equal(parseRange("bytes=-0", 1000), undefined);
  assert.equal(parseRange("bytes=1000-", 1000), undefined);
  assert.equal(parseRange("bytes=0-1,5-9", 1000), undefined);
});
