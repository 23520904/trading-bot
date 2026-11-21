---
name: design-conformance-review
description: Review code or a pull request against the CIP design pack and produce a pass/fail conformance report. Use for "/design-conformance-review <path|PR|service>".
---
# Design conformance review

Scope: the given service/path/diff. Read the relevant spec card (15/16), pages 06-08 and the L2/sequence pages. Do not modify code in this skill; report only.

Check and mark PASS / FAIL / N/A with file:line evidence:
1. Topics, keys, groups, headers exactly as page 06; no invented topics/fields.
2. Idempotent consumers; commit after sink; poison pill -> DLQ; no infinite retry.
3. Data ownership (page 08): no foreign-schema writes, no foreign Redis/MinIO prefixes; correct DB role.
4. Layer rules: gateway has no datastore access; clients never hit DBs; WS path as designed.
5. Candles/features only in Flink/Spark via `cip-core`; strategy logic only in `strategy-core`; no look-ahead.
6. Paper-only: no exchange order/account calls, no private keys.
7. Types and time: Decimal, UTC micros, `BASE-QUOTE` symbols.
8. Observability: health endpoints, metrics names, JSON logs, traceparent propagation.
9. Tests required by the spec card exist and pass; DoD items met.
10. Secrets, logging of sensitive data, destructive operations.

Output: a table (check, result, evidence), then "must fix", "should fix", and "design question" lists. Be concrete; do not pass items you did not verify.
