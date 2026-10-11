# KKAI v0.2.7 merge record

This worktree combines the official `v0.2.7` baseline with the local KKAI release customizations and the account-level STATE design from PR #7338.

The official account-level model is authoritative for ticket semantics: one manually opted-in account selects one Pro (292) or Team (332) plan and one outbound model. KKAI egress pools, lease admission, session affinity, warmup, dispatch budgets and migration contracts remain authoritative for transport and scheduling. Ticket state, configuration and watchdog summaries use the private `codex_turn_ticket:v2:` namespace and are persisted through the CAS/lease store; the old v1 keys remain readable for rollback and are never overwritten by v2 settings writes.

Before a release candidate is routed, operators must enable the v2 master, configure the shared harvest pool, and explicitly map every previously protected account to a plan and model. A legacy deployment that protected Astra and Sol globally cannot be represented by one v2 account without choosing a model or separating the accounts. This is a deliberate safety gate: an unconfigured account stays disabled rather than silently changing its ticket policy.

The merge does not add a SQL migration. The account store fences credentials, proxy and egress identity/revision, distinguishes missing values from JSON `null`, and uses a database-clock lease with an increasing fence. Blue-green rollback keeps the old pair and old ticket namespace intact. Production delivery remains a separate runbook operation after this worktree passes release gates; this merge turn performs no source push or deployment.

## Validation record

Targeted checks run in the merge worktree are listed in the task report. They cover service ticket lifecycle/watchdog/egress tests, repository CAS and lease tests (including the optional PostgreSQL concurrent collector test), admin settings/account handlers, frontend ticket/settings tests and `vue-tsc`, server wiring/routes, and migration invariants. The full repository suite was intentionally not run.
