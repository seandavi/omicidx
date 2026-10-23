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
