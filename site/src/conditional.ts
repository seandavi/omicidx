/**
 * The request headers to hand R2 as `onlyIf`. DuckDB-Wasm 2.0 (the
 * shell.duckdb.org build) sends `If-Match: %22<etag>%22`, percent-encoding the
 * quotes of the ETag it read, and R2 throws on that, so every read became a
 * 500 (#227). Decoding the quotes restores the ETag it meant.
 */
export function conditionalHeaders(headers: Headers): Headers {
  const out = new Headers(headers);
  for (const name of ["if-match", "if-none-match"]) {
    const v = out.get(name);
    if (v?.includes("%22")) out.set(name, v.replaceAll("%22", '"'));
  }
  return out;
}

/**
 * The status for a conditional GET that R2 refused (it hands back an R2Object
 * without a body and doesn't say which precondition failed). RFC 9110 §13.2.2:
 * a failed `If-Match` / `If-Unmodified-Since` is 412; only a failed
 * `If-None-Match` / `If-Modified-Since` is 304.
 */
export function failedConditionStatus(
  headers: Headers,
  etag: string,
  uploaded: Date,
): 304 | 412 {
  const ifMatch = headers.get("if-match");
  if (ifMatch !== null) {
    const tags = ifMatch.split(",").map((t) => t.trim());
    if (!tags.includes("*") && !tags.includes(etag)) return 412;
  } else {
    const since = Date.parse(headers.get("if-unmodified-since") ?? "");
    // HTTP dates have 1 s resolution; R2's upload time has ms.
    if (since && Math.floor(uploaded.getTime() / 1000) * 1000 > since) return 412;
  }
  return 304;
}
