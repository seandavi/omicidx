/** A byte range to read: offset + length, already clamped to the object. */
export type ByteRange = { offset: number; length: number };

/**
 * Parse an HTTP Range header (single range only) against an object of
 * `totalSize` bytes. Handles `bytes=a-b`, `bytes=a-` and the suffix form
 * `bytes=-n` (last n bytes), which object_store/Arrow use to read Parquet
 * footers (#213). Returns undefined for anything unsatisfiable or
 * unsupported; the caller then serves the full body.
 */
export function parseRange(header: string, totalSize: number): ByteRange | undefined {
  const suffix = header.match(/^bytes=-(\d+)$/);
  if (suffix) {
    const n = Math.min(parseInt(suffix[1], 10), totalSize);
    return n > 0 ? { offset: totalSize - n, length: n } : undefined;
  }
  const match = header.match(/^bytes=(\d+)-(\d*)$/);
  if (!match) return undefined;
  const start = parseInt(match[1], 10);
  let end = match[2] ? parseInt(match[2], 10) : totalSize - 1;
  if (start >= totalSize || end < start) return undefined;
  if (end >= totalSize) end = totalSize - 1; // clamp to EOF
  return { offset: start, length: end - start + 1 };
}
