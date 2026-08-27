# Traffic Director V1 Rollout and Rollback

Traffic Director V1 is a Group-scoped OpenAI routing policy. A Group remains
`legacy` after migration unless an administrator explicitly publishes a
policy. The first release does not change billing, account configuration,
non-OpenAI adapters, or the scheduler's scoring weights.

## Safety rules

- Upgrade all application instances before enabling `enforced` on any Group.
- Use `legacy` as the immediate compatibility baseline, then `shadow`, then a
  small enforced percentage represented by pool weights.
- A pool can fall back only through its declared `fallback_pool_key` chain.
  Exhaustion returns `503 TRAFFIC_DIRECTOR_NO_AVAILABLE_POOL`; accounts from
  unrelated pools are never selected.
- A hard `previous_response_id` continuation may cross a pool boundary. The
  decision is logged with reason `hard_previous_response`.
- Unassigned Group accounts are excluded from new routing. `enforced` publish
  requires `confirm_unassigned_accounts=true`.
- Health state is account+model scoped in Redis. Redis failure fails open only
  for health filtering and never permits crossing a pool boundary.

## Staged rollout

1. **Baseline:** verify every target Group reports version `0`, mode `legacy`.
   Record legacy selection errors, capacity-limited responses, and scheduler
   latency for at least one normal traffic window.
2. **Shadow:** publish the intended policy with mode `shadow`. Shadow computes
   the weighted rendezvous home Pool once and compares it with the account
   selected by the unchanged legacy scheduler. Review structured
   `openai.traffic_director.shadow_decision` logs; they contain pool names and
   account IDs but no routing key, token, or request body.
3. **Canary:** publish the same policy as `enforced` with the canary weight
   (normally 1%). Keep a zero-weight backup Pool explicitly linked from every
   normal Pool that needs failover.
4. Increase the canary through `5%`, `20%`, `50%`, and `100%`, pausing at each
   step to review `TRAFFIC_DIRECTOR_NO_AVAILABLE_POOL`, policy-unavailable
   responses, pool distribution, upstream failures, and health state changes.
5. **Completion:** retain the immutable history and the previous version as
   the rollback target. Do not edit old history rows or reuse an idempotency
   key for a different payload.

## Publish checklist

- Fetch the current Group head and use its `expected_version`.
- Run Preview and review checksum, normalized Pool order, fallback chains,
  account membership, and `unassigned_account_ids`.
- Ensure normal Pool weights sum to `10000` basis points. Zero-weight Pools
  must be referenced by a fallback chain.
- Send a fresh `Idempotency-Key` for the publication. Retrying the exact same
  request is safe; reusing the key with a different fingerprint returns a
  conflict.
- For enforced mode, set `confirm_unassigned_accounts=true` only after the
  unassigned list has been reviewed.
- After a successful response, confirm the new version is the Group head and
  that API-key auth snapshots have been invalidated. The durable outbox is the
  crash-recovery path if the immediate invalidation is unavailable. Snapshot
  invalidation is eventually consistent across instances: before accepting an
  Enforced rollout step, confirm the auth invalidation outbox has no pending
  items, then wait at least the maximum configured API-key L1 TTL (including
  its jitter allowance) on every instance. The authenticated Group snapshot is
  the policy consistency boundary for each request during this window.

## Rollback

Rollback is a new publication, never an update or delete of history:

1. Read the current head and select the known-good historical version.
2. Call the rollback endpoint with the current `expected_version`, a new
   `Idempotency-Key`, and the target version in the URL.
3. Review the returned version, checksum, mode, and unassigned accounts.
4. If the target is enforced and leaves accounts unassigned, explicitly set
   `confirm_unassigned_accounts=true`.

The database creates version `N+1` with `rollback_from_version=N_target` and
advances the Group head atomically with scheduler outbox publication. A
version conflict (`409 TRAFFIC_DIRECTOR_VERSION_CONFLICT`) means another
operator published first; reload the head rather than retrying with a stale
version.

## Failure handling

- `503 TRAFFIC_DIRECTOR_POLICY_UNAVAILABLE`: enforced routing could not obtain
  the exact immutable policy version. Keep the Group in legacy or shadow until
  the policy cache/Redis/PostgreSQL path is healthy.
- `503 TRAFFIC_DIRECTOR_NO_AVAILABLE_POOL`: the selected Pool and its explicit
  fallback chain have no qualifying account. Check schedulability, capacity,
  health state, model capability, and `min_available`; do not add an implicit
  cross-pool fallback.
- `422 TRAFFIC_DIRECTOR_VALIDATION_FAILED`: fix the Preview errors. Common
  causes are duplicate account assignments, invalid weights, a fallback cycle,
  missing Group membership, or an unconfirmed unassigned account.

Health transitions are `healthy -> suspect -> open -> half_open -> healthy`.
The first qualifying account-scoped failure is `suspect`; the second opens the
account for 10 seconds, and a failed half-open probe opens it for 45 seconds.
429/529, authentication errors, quota errors, unsupported models, and client
errors do not trip this circuit. Health state is not stored in PostgreSQL and
does not mutate `schedulable`, `priority`, or `load_factor`.

## Observability

Use low-cardinality counters and latency measurements for policy resolution,
pool decisions, fallback exhaustion, health fail-open, and policy-unavailable
errors. Structured logs may include Group, Pool, account, version, and a
bounded reason. Never log routing keys, credentials, tokens, or complete
request bodies.

Before declaring a rollout step complete, verify:

- same routing key is stable under an unchanged policy;
- a fixed 100,000-key sample stays within the expected weight distribution;
- enforced selections are inside the current Pool except logged hard previous
  response overrides;
- legacy and non-OpenAI requests retain their existing selection behavior;
- new instances do not remove live concurrency slots or shared wait counts;
- candidate policy reads hit process LRU without Redis/PostgreSQL on the hot
  path;
- the previous immutable version can be published as a new rollback version.

Production image build and blue-green deployment are separate operations and
require explicit authorization. Follow the repository Sub2API deployment
runbook for those operations; this document only defines Traffic Director
application rollout semantics.
