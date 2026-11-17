# Application Manager

The Application Manager learns which applications are installed in an
environment. A deployment tool seeds an application by sending its manifest:
an envelope with the application id and version, and one section per section
key. The gear checks the manifest, accepts the request as an operation, and
dispatches each section to the section plugin that owns its key, in priority
order. An uninstall asks every section plugin to remove the application.

- Design: [docs/DESIGN.md](docs/DESIGN.md)
- Requirements: [docs/PRD.md](docs/PRD.md)
- Decisions: [docs/ADR/](docs/ADR/)
- First steps: [QUICKSTART.md](QUICKSTART.md)

| Crate | Holds |
|---|---|
| [application-manager-sdk](application-manager-sdk/) | The section plugin contract, its GTS types, and the outcome report models |
| [application-manager](application-manager/) | The gear |
| [plugins/noop-section-plugin](plugins/noop-section-plugin/) | A section plugin that accepts any object; a working reference |

## For section plugin authors

The normative contract is
[`SectionPluginClientV1`](application-manager-sdk/src/section_plugin_api.rs):
read it before writing a plugin. In short:

1. **Register a section type** in your gear's `init`: a GTS type derived
   directly from `gts.cf.core.application_manager.section.v1~`, as one
   segment `<vendor>.<package>.<namespace>.<type>.v<major>[.<minor>]~`. Its
   schema is your section's content rules, as JSON Schema Draft-07, with every
   `$ref` inside the document.
2. **Register one plugin instance** of
   `gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~`
   with your `priority` and `properties.section_type`, the exact GTS id of
   your section type.
3. **Register a scoped `ClientHub` client** for `SectionPluginClientV1` under
   your instance id.

The no-op plugin does all three in
[`gear.rs`](plugins/noop-section-plugin/src/gear.rs).

**Section key.** The key is your type's segment without its version:
`gts.cf.core.application_manager.section.v1~acme.iam.access.roles.v1.2~` is
served under `acme.iam.access.roles`. Two versions of one type share a key, so
only one plugin may register it.

**Priority.** Seeds call `apply` in ascending priority; uninstalls call
`remove` in the same order. Priorities are unique across plugins; two equal
priorities fail startup. Pick a priority after the plugins whose data yours
refers to, for example after the plugin that creates the roles your section
grants.

**Obligations.**

- Use your own service credentials; the gear passes the caller's identity,
  never a credential.
- Own rollback, idempotency, and the meaning of versions. Calls can repeat,
  and an uninstall arrives whether or not the application was seeded.
- Return within `plugin_call_timeout`. A call past it is dropped and recorded
  as `timed_out`, and its effect is unknown; keep your data consistent when
  that happens.
- Refuse content with `Rejected` (with an optional JSON Pointer `location`),
  refuse the caller with `NotPermitted`, and report your own failures with
  `Internal`. `Rejected` and `NotPermitted` detail reaches the caller;
  `Internal` text is only logged.

The plugin set is fixed once at the gear's `start`. A plugin that registers
later is not used until the gear restarts.

## For operators

### Configuration

Settings live under `gears.application-manager.config`. Each is required; a
missing or invalid value fails `init` with `invalid_setting`.

| Setting | Meaning |
|---|---|
| `plugin_call_timeout` | Time limit of every plugin call |
| `run_window_margin` | Added to every operation's run window |
| `operation_retention` | How long a completed operation stays readable |
| `plugin_max_in_flight` | Calls in flight per plugin |
| `max_request_tasks` | Seed and uninstall requests in flight per replica |
| `max_envelope_text_length` | Maximum bytes of an application id and version (at most 255) |
| `pdp_call_timeout` | Deadline of one PDP decision for the tenant-plane reads |
| `allowed_callers` | The platform identities allowed to seed, uninstall, and read schemas and operations |
| `storage_call_timeout` | Bound of every storage access; 3 × this must fit the gateway's 30 s deadline |

```yaml
gears:
  application-manager:
    database: { server: "pg_main" }
    config:
      plugin_call_timeout: "10s"
      run_window_margin: "30s"
      operation_retention: "7d"
      plugin_max_in_flight: 4
      max_request_tasks: 16
      max_envelope_text_length: 255
      pdp_call_timeout: "2s"
      allowed_callers:
        - identity: { type: KubernetesServiceAccount, namespace: deploy, service_account: app-seeder }
          actions: [seed, uninstall, read_schema, read_operation]
        - identity: { type: Spiffe, trust_domain: prod.example, name: ci/release }
          actions: [read_schema, read_operation]
      storage_call_timeout: "2s"
```

An allowed-callers entry matches a Kubernetes service account in any pod, or
a SPIFFE identity at any version. The actions are `seed`, `uninstall`,
`read_schema`, and `read_operation`. The history read and the application
list are tenant-plane reads, decided by the platform's access policy as the
permissions `read_history` and `read_applications` on
`gts.cf.core.application_manager.application.v1~`.

**Retention.** A completed operation is purged once `operation_retention` has
passed since it finished. Keep it longer than the longest time a deployment
tool may poll an operation after it ends. The history and the application
list are never purged.

### Answers and when to retry

A seed or uninstall answers synchronously, then runs as an operation that
`GET /operations/{operation_id}` reports on.

| Answer | Meaning | Retry |
|---|---|---|
| 202 | Accepted; poll the operation | — |
| 503 | Not ready, at capacity, or storage cannot answer | Yes, after `Retry-After` |
| 403 | The caller is not allowed for the action | No; obtain the permission |
| 409 | Another operation of the application is open; the body names it | Yes, once that operation is final |
| 400 | The envelope is invalid, or the uninstall id is outside the limits | No; fix the request |
| 500 | The request failed inside the gear | Yes, send again |

| Operation category | Meaning | Retry |
|---|---|---|
| `succeeded` | Every section, or every plugin for an uninstall, succeeded | — |
| `invalid_manifest` | A section does not match its schema | No; fix the named sections |
| `not_permitted` | A plugin refused the caller | No; obtain the plugin's permission |
| `plugin_rejected` | A plugin refused its section | No; fix the named sections |
| `internal_failure` | A plugin failed, timed out, or was not called, or the operation expired | Yes, send again |

### Background work and shutdown

Every replica runs an operation sweeper. Every 30 seconds it completes up to
100 open operations whose run deadline has passed, as `internal_failure` with
reason `expired`. Every hour it purges up to 100 completed operations past
`operation_retention`. On stop, the gear refuses new requests, waits for the
requests in flight until the host's stop deadline, and stops the sweeper. An
operation still open at that point is completed by the sweeper of any replica
once its run deadline passes.

### Metrics

Meter `application_manager`. Section keys are labels; application and
operation ids never are.

| Metric | Labels |
|---|---|
| `application_manager_requests_total` | `action`, `outcome` (`accepted` or the refusal category) |
| `application_manager_operations_completed_total` | `action`, `category`, `reason` (`run`, `supervisor`) |
| `application_manager_operation_duration_seconds` | `action` |
| `application_manager_section_calls_total` | `section_key`, `outcome` |
| `application_manager_caller_refusals_total` | `action` |
| `application_manager_request_tasks` | — (in flight) |
| `application_manager_history_events_total` | `result` (`recorded`, `not_recorded`) |
| `application_manager_dependency_unavailable_total` | `dependency` (`storage`, `pdp`) |
| `application_manager_sweeps_total` | `step` (`expire`, `purge`), `result` |
| `application_manager_operations_expired_total` | — |
| `application_manager_startup_failures_total` | `reason` |

Alert on:

- any `not_recorded` history event;
- a rising `operations_expired_total`;
- repeated `timed_out` or `not_sent` section outcomes for one key;
- any `dependency_unavailable_total`;
- failing sweeps;
- `request_tasks` at `max_request_tasks`;
- any startup failure.

Startup failure reasons are `types_registry_unreachable`,
`plugin_properties_unreadable`, `section_type_unresolved`,
`invalid_section_type`, `section_schema_unusable`, `plugin_client_unresolved`,
`duplicate_section_key`, `equal_priority`, and `invalid_setting`.
