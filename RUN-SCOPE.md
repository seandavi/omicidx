# RUN-SCOPE — omicidx Stage A implementation run

Fixed spec: docs/specs/omicidx-deliverables.md. This run implements
STAGE A ONLY. The spec is the endpoint; do not redesign it.

## In scope (Stage A — internal steel thread)
- A1: extract narrow Source protocol; refactor 5 flows behind it.
- A2: extend internal-lake snapshot retention; document raw as
  re-derivation backstop.
- A3: reproduce-from-raw entrypoint + idempotency smoke test.
- A4: fix stale DUCKLAKE.md.

## Explicitly OUT of scope this run
Stage B, B', C, D, E, F. The external artifact, R2 publishing, the
frozen catalog, marts, docs, SQLMesh migration. Do not start these.

## Hard gates — STOP and ask the owner (never autonomous)
1. Any write to R2 (any bucket).
2. Any commit to CLAUDE.md (propose diff, wait).
3. Any retention/expiry change that COULD drop retained data. A2 must
   only extend; if any path shortens, HALT.
4. Any Worker / custom-domain / credential / R2-config change.
5. Any git push, tag, or release. Local commits on a work branch only.
6. Any SQLMesh / transform-engine migration (out of scope entirely).

Gates hold even under confidence, even if the spec seems to call for it,
even if a reviewer suggests it. Per-action, this session only.

## Spec-mismatch protocol
If implementation reveals the spec is wrong or underspecified (not merely
inconvenient): HALT, state the gap, ask whether to amend spec or proceed.
Do not silently patch around the spec.

## Definition of done
All 4 work units done; idempotency smoke test passes; DUCKLAKE.md clean;
all 4 personas pass OR objections recorded-and-overruled with rationale;
RUN-LEDGER.md complete. Then halt and present branch diff + ledger +
smoke-test output + dissents.
