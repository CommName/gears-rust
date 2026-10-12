---
status: proposed
date: 2026-10-12
description: The gear does not use the platform root tenant; the registry is an unrestricted global table, and permission decisions name no context tenant, so the policy decides who holds the platform-level permission
---
# Keep Platform Scope Without the Platform Root Tenant


<!-- toc -->

- [Context and Problem Statement](#context-and-problem-statement)
- [Decision Drivers](#decision-drivers)
- [Considered Options](#considered-options)
- [Decision Outcome](#decision-outcome)
  - [Consequences](#consequences)
  - [Confirmation](#confirmation)
- [Pros and Cons of the Options](#pros-and-cons-of-the-options)
  - [No root tenant: unrestricted registry and decisions with no context tenant](#no-root-tenant-unrestricted-registry-and-decisions-with-no-context-tenant)
  - [Eager root tenant lookup in start (fail fast)](#eager-root-tenant-lookup-in-start-fail-fast)
  - [Lazy root tenant lookup per request with a not-ready refusal](#lazy-root-tenant-lookup-per-request-with-a-not-ready-refusal)
- [More Information](#more-information)
- [Traceability](#traceability)

<!-- /toc -->

**ID**: `cpt-cf-application-manager-adr-platform-scope-without-root-tenant`

## Context and Problem Statement

The Application Manager keeps one platform-wide registry, and its four actions (seed, uninstall, listing, and schema reading) are platform-level actions ([PRD](../PRD.md) `cpt-cf-application-manager-nfr-platform-scope`, `cpt-cf-application-manager-fr-authorization`). Earlier drafts scoped both the registry and every permission decision to the platform root tenant. Each registry row carried the root tenant id as its secure-ORM scoping value, and each decision named the root tenant as its context tenant.

The root tenant does not exist when this gear starts on a fresh environment. Account-management creates it in a bootstrap saga that runs in its lifecycle `serve` task, after its `start()` has returned ([account-management gear.rs](../../../account-management/account-management/src/gear.rs), lines 91-96 and 208-218). The lifecycle wrapper's `start()` spawns that task and returns without waiting for the saga ([lifecycle.rs](../../../../../libs/toolkit/src/lifecycle.rs), lines 597-624). A gear dependency orders only the `start()` calls, so no dependency can make the root tenant exist when this gear's `start` runs.

Startup has no retry: every startup check fails `start` at once (`cpt-cf-application-manager-fr-startup-checks`). A root tenant lookup in `start` therefore races account-management. Moving the lookup into the request path, as the previous draft did, avoided the race. It added a not-ready refusal before the root tenant was known, an extra registry time bound in the request budget, a degraded healthcheck, and new signals. It also blocked discovery, because the `read_schema` decision needed the root tenant as its context.

How should the gear keep its registry and its permission decisions platform-wide without depending on a tenant that another gear creates later?

## Decision Drivers

* Every startup check fails `start` at once, with no retry, and no startup step may wait for another gear's background work (`cpt-cf-application-manager-fr-startup-checks`).
* Installed applications are a property of the environment: the registry is one platform-wide set, reachable only through the platform's access checks, with no tenant owner (`cpt-cf-application-manager-nfr-platform-scope`).
* No registry access may happen without a permit, and there is no raw SQL outside migrations ([DESIGN §2.2](../DESIGN.md#22-constraints)).
* Discovery and listing must be gated by their own permission decisions only, with no extra prerequisite (`cpt-cf-application-manager-fr-discovery`, `cpt-cf-application-manager-nfr-availability`).
* A request must fit the gateway deadline, so every extra bounded call in the request path costs budget (`cpt-cf-application-manager-nfr-bounded-request-time`).
* The gear must fail closed when access control cannot answer or answers in a form it cannot apply (`cpt-cf-application-manager-fr-dependency-failures`).
* The gear should own as little machinery as possible: no caches, no extra ports, and no dependencies that exist for one value.

## Considered Options

* No root tenant: unrestricted registry and decisions with no context tenant
* Eager root tenant lookup in start (fail fast)
* Lazy root tenant lookup per request with a not-ready refusal

## Decision Outcome

Chosen option: "No root tenant: unrestricted registry and decisions with no context tenant", because it removes the dependency on account-management's background work entirely. It keeps startup fail-fast, adds nothing to the request path, and leaves the platform-level permission where it belongs, in the authorization policy.

The decision has these parts:

* **Registry storage.** The `application_registry` entity is a truly global table declared `#[secure(unrestricted)]` ([AuthN, AuthZ and secure ORM](../../../../../docs/toolkit_unified_system/06_authn_authz_secure_orm.md#unrestricted-entities-secureunrestricted)). It has no `owner_tenant_id` column and no other scoping column, and `secure_insert` needs no tenant id. Access to it is gated only by the permission decision for the action.
* **Permission requests.** Every decision for `seed`, `uninstall`, `list`, and `read_schema` is sent with no context tenant, no tenant-mode override, and `require_constraints = false`. The PDP, which is the authz-resolver and its plugin, decides from the caller and the action by its own rules. When no context tenant is set, it may fall back to the subject's own tenant ([enforcer.rs](../../../../system/authz-resolver/authz-resolver-sdk/src/pep/enforcer.rs), lines 419-423).
* **Permit and scope.** The application resource type declares no supported constraint property. A permit with no constraint compiles to the unscoped (allow-all) scope, and the gear runs registry queries under it ([compiler.rs](../../../../system/authz-resolver/authz-resolver-sdk/src/pep/compiler.rs)). A permit that carries any constraint cannot compile and fails closed as not permitted. This matches the secure-ORM rule that a scope with tenant ids is denied on an unrestricted entity.
* **Policy responsibility.** Deciding who may perform these platform-level actions belongs to the authorization policy. Because the PDP may fall back to the caller's own tenant, the policy must grant the four actions only through a platform-level permission. A tenant-level role, such as the administrator of any tenant, must not grant them. The DESIGN records this as a platform dependency owned by the authz policy owners.
* **What is removed.** The root tenant holder, its port and adapter, the `tenant_resolver` gear dependency, the not-ready refusal before the root tenant is known, the degraded healthcheck, the `root_tenant_unresolved` log, metric, and alert cause, and the extra registry bound in the request budget. The gear declares `deps = [types_registry, authz_resolver]`.
* **Request budget.** A seed takes at most `pdp_call_timeout` + 2 × `registry_operation_timeout` + (N + 3) × `plugin_call_timeout`, and an uninstall the same with N + 2. `init` checks `pdp_call_timeout` + 2 × `registry_operation_timeout` + 3 × `plugin_call_timeout` against the 30s gateway deadline.
* **Readiness.** The gear is not ready until its startup result is published, and ready after that. Discovery is gated by its `read_schema` decision like every other read.

Decision scope: this ADR covers how the gear keeps its registry and its permission decisions platform-wide. It does not decide what the registry stores ([ADR 0005](./0005-cpt-cf-application-manager-adr-minimal-application-registry.md)), how outcomes map to HTTP statuses ([ADR 0006](./0006-cpt-cf-application-manager-adr-outcome-http-mapping.md)), or the content of the authorization policy, which its owners decide.

### Consequences

* Startup is simpler: no startup step depends on another gear's background work, so a fresh environment starts like any other, and there is no first-start race with account-management.
* The request path is simpler: no lookup, no cache, and no not-ready refusal beyond readiness, stopping, and the request-task cap. Discovery and listing need only their own decisions.
* The gear has no `tenant_resolver` dependency and no anonymous platform call.
* The policy must require a platform-level permission for the four actions. If a policy grants them to a tenant-level role, that role gets platform-wide seed, uninstall, and read access. The gear cannot detect how a permit was granted, so this is a recorded risk in the PRD and a platform dependency in the DESIGN.
* A policy that answers these actions with a constraint, such as a tenant predicate, makes every such request not permitted (403). The failure is visible and closed, never a silent widening.
* Any future need for tenant-owned applications requires a new ADR that supersedes this one and a migration that adds a scoping column to the registry table.

Impact by concern:

* Performance: one fewer bounded call in the request path, and one fewer `registry_operation_timeout` in the budget.
* Security: no tenant is asserted on the caller's behalf. Access rests on the policy's platform-level permission, and a constrained permit fails closed.
* Reliability: startup no longer races account-management, and no request is refused because another gear has not finished its work.
* Data: the registry holds no tenant value. Its rows hold only the fields that ADR 0005 lists.
* Integration: the gear depends on `types_registry` and `authz_resolver` only. The authorization policy must define the platform-level permission for the application resource type.
* Operations: there is no degraded state and no root-tenant signal to watch. Readiness means the startup result is published.
* Testing: see Confirmation.
* Compliance: no change; the registry holds no personal data.
* UX: callers on a fresh environment are served as soon as the replica is ready.
* Business: platform operators control who may seed and uninstall in one place, the authorization policy.

### Confirmation

* Inspection shows that the registry entity is declared `#[secure(unrestricted)]`, that the table has no owner column and no other scoping column, and that there is no raw SQL outside migrations.
* Unit tests show that every decision request names no context tenant and no tenant mode, and sets `require_constraints = false`.
* Integration tests show that a constraint-free permit reaches the registry under the unscoped scope, and that a permit with a tenant predicate, or with any other constraint, gives not permitted with no registry access.
* Startup tests show that `start` makes no tenant service call, and that the healthcheck reports not ready before publication and ready after it.
* Review with the authz policy owners confirms that the policy grants the four actions only through a platform-level permission ([DESIGN §4.12](../DESIGN.md#412-assumptions-dependencies-and-migration-impact), platform dependency 7).

## Pros and Cons of the Options

### No root tenant: unrestricted registry and decisions with no context tenant

Keep the registry in an unrestricted global table, request every decision with no context tenant, and let the policy decide who holds the platform-level permission.

* Good, because no startup step and no request depends on account-management's background work.
* Good, because it removes a port, an adapter, a cache, a gear dependency, a refusal path, and a set of signals.
* Good, because the request budget loses one registry bound, so more of the gateway deadline is left for plugin calls.
* Good, because discovery is gated by its own decision, like every other read.
* Good, because it uses the secure-ORM form that the platform guide defines for truly global tables.
* Neutral, because the gear still fails closed on an unanswered decision and on a constrained permit.
* Bad, because the gear relies on the policy to require a platform-level permission. A policy that grants these actions to a tenant-level role would widen access, and the gear cannot detect it.

### Eager root tenant lookup in start (fail fast)

Look up the root tenant once in `start`, and fail the start when it cannot be found.

* Good, because the startup rule stays uniform: every startup dependency is checked in `start` and fails it at once.
* Good, because the request path needs no lookup once startup has passed.
* Bad, because it races account-management, whose bootstrap saga creates the root tenant after its own `start()` has returned.
* Bad, because all gears run in one process, so a failed `start` aborts the host and account-management's half-finished bootstrap with it. With resource-group-backed tenant resolution, the root tenant appears only after that saga, so a fresh deploy can restart-loop.
* Bad, because a gear dependency cannot fix the race, since it orders only the `start()` calls.

### Lazy root tenant lookup per request with a not-ready refusal

Look up the root tenant on the first request that needs it, cache only a success, and refuse every request as not ready until then.

* Good, because it works: the gear starts on a fresh environment and serves once the root tenant exists, with no restart.
* Bad, because it adds machinery: a holder with a one-time cache, a port and an adapter, an extra gear dependency, and a not-ready refusal inside the request.
* Bad, because it adds one `registry_operation_timeout` to the request budget and to the `init` budget check.
* Bad, because it needs a degraded healthcheck that keeps the replica in rotation, plus new log, metric, and alert causes.
* Bad, because discovery is blocked until the root tenant is known, since the `read_schema` decision needs it as context.

## More Information

Related decisions:

* [ADR 0005](./0005-cpt-cf-application-manager-adr-minimal-application-registry.md): the registry's stored fields. With this decision, the table has no owner column and no other scoping column.
* [ADR 0001](./0001-cpt-cf-application-manager-adr-section-plugin-fan-out.md): the plugin set is resolved once at startup, and every startup fault fails the start, which is why a start-time root tenant lookup could not wait.
* [ADR 0006](./0006-cpt-cf-application-manager-adr-outcome-http-mapping.md): a decision that cannot be answered is temporarily unavailable (503), and a refused or constrained permit is not permitted (403).

Review and supersession: revisit this decision if per-tenant installs enter scope, if the platform defines a platform-scope convention for permission decisions, or if the authorization policy cannot express a platform-level permission for the application resource type. Any such change would supersede this ADR.

## Traceability

- **PRD**: [PRD.md](../PRD.md)
- **DESIGN**: [DESIGN](../DESIGN.md)

This decision directly addresses the following requirements or design elements:

* `cpt-cf-application-manager-fr-startup-checks` — no startup step depends on another gear's background work, so every check still fails `start` at once.
* `cpt-cf-application-manager-nfr-platform-scope` — one unrestricted table with no tenant owner, no registry access without a permit, and no raw SQL outside migrations.
* `cpt-cf-application-manager-fr-authorization` — decisions name no context tenant, and the policy grants the four actions only through a platform-level permission.
* `cpt-cf-application-manager-actor-platform-access-control` — the platform access control dependency in [PRD §10](../PRD.md#10-dependencies) carries the obligation on its policy.
* `cpt-cf-application-manager-nfr-bounded-request-time` — the request budget drops the extra registry bound.
* [DESIGN §3.2](../DESIGN.md#32-component-model) — no root tenant holder, port, or adapter; the healthcheck is not ready until publication and ready after it.
* [DESIGN §3.3](../DESIGN.md#33-api-contracts) — the authorization rules: no context tenant, `require_constraints = false`, and a constrained permit is not permitted.
* [DESIGN §3.7](../DESIGN.md#37-database-schemas--tables) — the registry table is `#[secure(unrestricted)]` with no owner column.
* [DESIGN §4.12](../DESIGN.md#412-assumptions-dependencies-and-migration-impact) — platform dependency 7, owned by the authz policy owners.
* `cpt-cf-application-manager-adr-minimal-application-registry` — ADR 0005, amended to match.
* `cpt-cf-application-manager-dbtable-application-registry` — the table that this decision makes unrestricted.
* `cpt-cf-application-manager-constraint-secure-orm` — registry access through SecureConn on an unrestricted entity.
