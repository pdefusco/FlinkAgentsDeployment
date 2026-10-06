# Monitoring a NiFi flow through the Cloudera DataFlow APIs

What the `df` and `dfworkload` APIs can and cannot tell you about a **running** NiFi flow on Cloudera
DataFlow Public Cloud (DFX): at what granularity, which metrics, over what time windows, and from
which network.

This exists because the question "can I monitor an individual processor?" has a precise answer that
**is not in the published API documentation**. Both HTML pages
([df](https://cloudera.github.io/cdp-dev-docs/api-docs/df/index.html) ·
[dfworkload](https://cloudera.github.io/cdp-dev-docs/api-docs/dfworkload/index.html)) truncate before
the KPI and metric schema definitions, so the scope enum and the metric-chart shape are simply absent
from them.

## Status and provenance

The authoritative source is the **OpenAPI spec bundled with the CDP CLI**, not the HTML docs:

```
$(python3 -c 'import cdpcli,os;print(os.path.dirname(cdpcli.__file__))')/data/df/df.yaml
$(python3 -c 'import cdpcli,os;print(os.path.dirname(cdpcli.__file__))')/data/dfworkload/dfworkload.yaml
```

Everything below carries its evidence. Three tiers, and they are not interchangeable:

| Tier | How it is marked | What it means |
|---|---|---|
| **Spec** | a `df.yaml:NNNN` / `dfworkload.yaml:NNNN` citation | Read out of the bundled OpenAPI. Line numbers are for **cdpcli 0.9.164**; schema names stay valid across versions, line numbers will not |
| **Measured** | `measured 2026-10-05` inline | Established by running against a live DFX deployment. Numbers are observations, not guarantees |
| **Unverified** | the word **unverified**, in bold | Stated because it is useful, flagged because nobody has checked. One item below — the Prometheus `/federate` workaround, unverified on two counts: its reachability, and where its password comes from |

Check which version your line numbers belong to with `cdp --version`.

---

## The short answer

**Granularity is five scope types.** They are enumerated identically in two places — `KpiScopeMetaData.type`
(`dfworkload.yaml:4284`) and `ConfiguredKpi.metricComponentType` (`dfworkload.yaml:2456`):

```
SYSTEM · NIFI_FLOW · NIFI_PROCESSOR · NIFI_PROCESS_GROUP · NIFI_CONNECTION
```

So yes — **processor, process group and connection are all first-class monitoring scopes.** Flow and
host-level system metrics sit alongside them. What differs between them is how much work it takes to
get a number out, and whether you can tell which component it belongs to:

| Scope | Alert on it | Read a number back | Component identified in the response |
|---|---|---|---|
| `SYSTEM` | yes | yes, configure nothing | n/a |
| `NIFI_FLOW` | yes | yes, configure nothing | n/a |
| `NIFI_PROCESSOR` | yes, via a KPI | **only if a KPI exists** | name only — no ID |
| `NIFI_PROCESS_GROUP` | yes, via a KPI | **only if a KPI exists** | name only — no ID |
| `NIFI_CONNECTION` | yes, via a KPI | **only if a KPI exists** | name only — no ID |

**But there is no "read all processors" call.** The API surfaces a per-component metric only where
somebody has explicitly configured a **KPI** against that component. A KPI is a persistent
configuration object, not a query parameter. Nothing in `df` or `dfworkload` scrapes a processor you
have not already declared interest in.

**And the metric catalogue is discovered at runtime, not documented.** No metric ID string appears
anywhere in either YAML file — not one. The list of legal metrics per scope comes back from a live
call, which (see below) runs only from inside the environment's VPC.

Three limits are worth knowing before you design anything against this:

| Limit | Evidence |
|---|---|
| `metricsTimePeriod` is the **only** windowing knob: `LAST_THIRTY_MINUTES`, `LAST_ONE_HOUR`, `LAST_TWELVE_HOURS`, `LAST_ONE_DAY`. No from/to, no component filter, no metric filter | `ListDeploymentKPIsRequest` `df.yaml:3059` · `ListFlowKpisInDeploymentRequest` `df.yaml:3219` |
| `MetricChart` carries `componentName` but **not** `componentId`. The only schema with `componentId` is `MetricSummary`, reachable solely through the event-detail calls | `df.yaml:1694` vs `df.yaml:1796` |
A KPI breach is the **only** cause of `CONCERNING_HEALTH` the spec names: *"there is a concern with the health of the flow (e.g., a KPI threshold breach)"*. Read that as one illustrative cause, not a definition — it does **not** follow that a flow with no KPIs can never report concerning health | `DeployedFlowState` `df.yaml:3324` |

---

## Two APIs, two networks

This is the first thing that will cost you time, and it is not an auth problem.

| | `cdp df …` | `cdp dfworkload …` |
|---|---|---|
| Plane | Control plane | Workload plane |
| Endpoint | Public | Private `internal-*` ELB **inside the environment VPC** |
| From a laptop | Works | **Times out** |
| Failure mode when unreachable | — | Hangs, then times out. It does **not** return an auth error |

**The discriminator: a printed workload-token expiry followed by a hang means the network is missing,
not your credentials.** A real credential or entitlement problem fails *before* a token is minted, so
you get an error with no expiry line at all. Learn to read that one line and you stop debugging IAM
when the problem is routing.

**The corporate VPN does not bridge the gap** — measured 2026-10-05. With the tunnel up,
`dfx.<env>.<region>.cloudera.site` resolves publicly to RFC1918 addresses (`10.10.0.157`,
`10.10.1.139`) routed into the tunnel, and they still time out after 15 s — while the public control
plane answers through the same tunnel in 0.06 s. The tunnel is healthy; packets are dropped silently
at the far end. **A timeout rather than a refusal is the tell**: a refusal means something answered
and declined. The only proven path to the workload plane is running inside the VPC — a Cloudera AI
session or job in the same environment.

One exception worth memorising: **`cdp df change-flow-version-in-deployment` is spelled `df` but hops
to the workload plane internally**, so it needs VPC reachability regardless.

> **What this means for a monitoring loop.** Every KPI *value* read is control-plane and works from a
> laptop: `describe-deployment`, `describe-flow-in-deployment`, `list-flow-kpis-in-deployment`,
> `get-flow-version`. The KPI *catalogue* read and **every mutation** are workload-bound. So you can
> build a read-only monitor anywhere, but anything that configures a KPI has to run in the VPC.

---

## What you can read, per scope

All of these are `cdp df` — control plane, laptop-reachable.

| Scope | Command | Response key | `componentName` | Needs a configured KPI |
|---|---|---|---|---|
| Any of the five | `list-flow-kpis-in-deployment` | `metricCharts` | yes, for processor / group / connection | **yes** |
| Any of the five | `list-deployment-kpis` | `metricCharts` | yes, as above | **yes** |
| System / infra | `list-deployment-system-metrics` | `metricCharts` | no | no |
| Service (the DFX service itself, not a flow) | `list-service-system-metrics` | `metricCharts` | no | no |

The split is the useful part: **system metrics come free, flow-internal metrics do not.** The two
`system-metrics` calls return charts whether or not you configured anything. The two `kpis` calls
return exactly the KPIs somebody declared — an unconfigured flow returns an empty array, not an error,
which is easy to misread as "the call is broken."

**There are three CRN axes, not two.** Almost every read here exists at service, deployment and
deployed-flow scope, and it is easy to miss the service tier because only its metrics call is obvious:
`describe-service`, `list-service-system-metrics`, `list-service-active-alerts`,
`list-service-events`, `describe-service-event-detail`. Service scope covers the DFX environment
itself — the Kubernetes substrate your deployments run on — not the flows inside it.

`list-flow-kpis-in-deployment` takes three arguments and **all three are required**:

```bash
cdp df list-flow-kpis-in-deployment \
  --deployment-crn    "$DEPLOYMENT_CRN" \
  --deployed-flow-crn "$DEPLOYED_FLOW_CRN" \
  --metrics-time-period LAST_THIRTY_MINUTES
```

There is nothing else to pass. No `--component-id`, no `--metric-id`, no `--from` / `--to`. If you
want one processor's chart out of a response, you filter client-side on `componentName`.

## The metric chart shape

`MetricChart` (`df.yaml:1694`) is what every one of those four calls returns an array of:

| Field | Type | Notes |
|---|---|---|
| `name` | string | The metric's name |
| `unitType` | string | One of `DURATION`, `RATIO`, `SIZE`, `RATE`, `COUNT` |
| `componentType` | string | The scope — one of the five |
| `componentName` | string | Spec: *"will exist for Processor, Process Group, and Connection metrics"* |
| `metrics` | `MetricChartData` | The series |
| `mirroredMetrics` | `MetricChartData` | Optional; spec: *"only exist for certain system metrics"* |
| `alert` | `MetricChartAlert` | The configured thresholds, echoed back |

`MetricChartData` (`df.yaml:1631`) carries both the series and its summary, so you do not have to
compute the obvious aggregates yourself:

```
averageValue · currentValue · currentValueLabel · averageValueLabel · tooltipValueLabel
datas[] → MetricValue { timestamp: int64, value: double }
```

`MetricChartAlert` (`df.yaml:1677`) echoes the KPI's own alert configuration — `thresholdMoreThan`,
`thresholdLessThan`, and a `frequencyTolerance` (`{unit, value}`) giving how long a value may sit
outside its bounds before an alert fires. **You get the threshold back with the data**, so a monitor
can evaluate breach locally without a second lookup of the KPI definition.

> **The spec contradicts itself on two of these fields.** `MetricChart`'s `required` list names
> `mirroredMetrics` and `alert`, while both fields' own descriptions call them optional
> ("The **optional** mirrored metrics…", "The **optional** thresholds…"). Code defensively and treat
> both as absent-able; do not let a schema generator talk you into asserting their presence.

### The 25-bucket trap

Measured 2026-10-05. **`list-flow-kpis-in-deployment` does not publish on a fixed cadence.** It
divides whatever window you asked for into exactly **25 buckets**, so the gap between points is
`window / 24`:

| `metricsTimePeriod` | Bucket width |
|---|---|
| `LAST_THIRTY_MINUTES` | 75 s |
| `LAST_ONE_HOUR` | 150 s |
| `LAST_TWELVE_HOURS` | 1800 s |
| `LAST_ONE_DAY` | 3600 s |

The familiar "75 seconds" is a consequence of asking for 30 minutes, nothing more.

**Buckets are anchored to request time.** So differencing a value across *two* responses compares
points on two different grids. Measured error doing exactly that: **~20%**. Differencing adjacent
points **inside a single response** is sound.

> The structural fix: make any function that computes a rate take **the already-fetched chart object**
> as its argument, never a CRN. Then a second fetch is impossible to write by accident.

**Second-order consequence: widening the window silently disables freshness checks.** At
`LAST_ONE_DAY` the newest bucket can be an hour old. A monitor with a 300 s staleness limit then
rejects every sample and reports `UNKNOWN` forever — without ever raising an error.

For calibration: on a synthetic drain flow, nine falling buckets differenced inside one response gave
**−1.99 files/s with essentially no scatter** (measured 2026-10-05). The method is precise. It is only
cross-response differencing that is not.

---

## Discovering the metric catalogue

Since no metric ID is documented, you enumerate them. **This call is workload-plane — VPC only.**

```bash
cdp dfworkload get-flow-configuration-metadata-in-deployment \
  --environment-crn    "$ENV_CRN" \
  --deployment-crn     "$DEPLOYMENT_CRN" \
  --deployed-flow-crn  "$DEPLOYED_FLOW_CRN"
```

The path down to the catalogue (`RpcDeployedFlowConfigurationMetadata` `dfworkload.yaml:5247` →
`KpiMetaData` `dfworkload.yaml:4234`):

```
deployedFlowConfigurationMetadata
└── kpiMetaData                       "A template for instantiating KPIs"
    ├── kpiScopes[]                   KpiScopeMetaData — "one per MetricComponentType"
    │   ├── type                      SYSTEM | NIFI_FLOW | NIFI_PROCESSOR | NIFI_PROCESS_GROUP | NIFI_CONNECTION
    │   ├── label                      display label for the scope
    │   ├── contextLabel               label for the context identifier, varies by scope type
    │   ├── metricTypes[]             KpiScopeMetricType  ← the metric catalogue for this scope
    │   │   ├── id                     the metric ID you put in ConfiguredKpi.metricId
    │   │   ├── label
    │   │   ├── unitTypeKey            DURATION | RATIO | SIZE | RATE | COUNT
    │   │   ├── defaultUnitId
    │   │   └── description
    │   └── contextGroups[]           KpiContextGroup — the component inventory
    │       ├── id, name               the containing process group
    │       └── scopeComponents[]     KpiScopeComponent { id, name } — nests recursively
    ├── unitTypes                     map: unitTypeKey → KpiUnit[] { id, label, factor }
    └── alertFrequencyTolerance       shared by all metrics
```

Two things to take from that tree:

- **`metricTypes[]` is the answer to "what metrics can I extract."** It is per-scope and it is live, so
  it tracks the NiFi version actually running rather than whatever the docs said.
- **`contextGroups[]` is a component inventory** — the hierarchy behind the UI's chooser, nesting
  recursively through process groups. It is how you discover the IDs of the processors and connections
  you might want to configure a KPI against.

`KpiUnit.factor` is the conversion factor to normalise values into a common unit — relevant because a
threshold you set in one unit comes back expressed in another.

**The sting: you cannot enumerate the available metrics from a laptop.** The values of KPIs you
already have read fine from the control plane, but the list of metrics you *could* configure does not.
So building a KPI set is a two-location job, and there is no documented enum to work from offline.

Note that `KpiMetaData`, `KpiScopeMetaData` and `ConfiguredKpi` are all marked `x-workload: true` and
`x-client-only: true` in the spec — they are workload-plane client shapes, not part of the public `df`
REST surface. That is consistent with the network split, and it is why you will not find them in the
`df` documentation no matter how hard you look.

---

## Alerts and events

Alerts are where per-component identity actually surfaces.

| Call | Returns | Notable arguments |
|---|---|---|
| `list-deployment-active-alerts` | `eventSummaries[]` | `sort` = `(firstOccurrence\|name\|eventType):(asc\|desc)` |
| `list-flow-active-alerts-in-deployment` | `eventSummaries[]` | same, plus `deployed-flow-crn` |
| `list-deployment-events` | `eventSummaries[]` + `nextToken` | `timestampFrom`, `filters`, `pageSize` ≤ 100 |
| `list-flow-events-in-deployment` | `eventSummaries[]` + `nextToken` | same, plus `deployed-flow-crn` |
| `describe-deployment-event-detail` | `EventDetail` | one event CRN |
| `describe-flow-event-detail-in-deployment` | `EventDetail` | one event CRN |

`EventSummary` (`df.yaml:1522`) is deliberately thin: `crn`, `name`, `severity`, `firstOccurrence`,
`eventType`, `alertType`. **No component, no metric value.** Legal values for `filters` come from
`cdp df list-filter-options` — guessing them yields an empty list rather than an error.

Note that the event calls are the **only** ones here with a real time parameter (`timestampFrom`) and
the only paginated ones. Metrics get four fixed windows; events get a timestamp. **This asymmetry is
the single most useful thing about the event surface** — if you need an arbitrary window over
anything, events are where you can have one.

> **Pagination trap.** `pageSize` is `1..100`. The older `rows` argument is marked `x-deprecated`, and
> it does not merely get ignored: the spec states that **if `max-items` is also specified and `rows`
> is less than `max-items`, an error is returned** (`df.yaml:3133`). Use `pageSize` and `startingToken`.

`Event` itself (`df.yaml:1551`) is much richer than `EventSummary`, and this is where threshold
context lives: `eventValue` (the value when it triggered), `lowerThreshold`, `upperThreshold`,
`lowerThresholdUnit`, `upperThresholdUnit`, `timeToleranceMillis`, `lastOccurrence`, `description`,
`userName`. **So an alert tells you the value that breached, the bound it broke, and the tolerance it
outlasted** — enough to act without fetching a chart at all.

One wart worth knowing before you go looking: **`Event.referenceType` is required and documented as
*"Type of component that is identified by the referenceId"* — and `Event` has no `referenceId`
property.** The pointer the description promises is not in the schema. Component identity comes from
`metricSummary` instead, below.

**The event-detail calls are the only route to `componentId`.** `EventDetail` (`df.yaml:3314`) is
`{event, metricSummary}`, and `MetricSummary` (`df.yaml:1796`) is the one schema in the whole surface
that carries it:

```
name · label · description · unitType · componentType · componentName
componentId        ← nowhere else in either API
values[]           → MetricValue { timestamp, value }
displayContext
```

So the shape of a working alert-driven monitor is: **list active alerts → describe the detail → read
`componentId` to learn which processor or connection actually breached.** You cannot get from a
`metricCharts` response to a component ID directly, because `MetricChart` does not carry one.

---

## Deployment and flow state

Two state enums, and picking the wrong one is a real trap.

**`DeploymentState`** (`df.yaml:2758`) — 20 values: `DEPLOYING`, `GOOD_HEALTH`, `CONCERNING_HEALTH`,
`BAD_HEALTH`, `STARTING_FLOW`, `SUSPENDED`, `UPDATING`, `TERMINATING`, `RESTARTING`, `UPGRADING`,
`ROLLING_BACK`, `STOPPED`, `UNKNOWN`, `IMPORTING_FLOW`, `STOPPING_FLOW`, `FLOW_STOPPED`, `SUSPENDING`,
`RESUMING`, `CHANGING_FLOW_VERSION`, `TERMINATED`.

Six of those are marked `x-deprecated-enum-values`: `STARTING_FLOW`, `STOPPED`, `IMPORTING_FLOW`,
`STOPPING_FLOW`, `FLOW_STOPPED`, `CHANGING_FLOW_VERSION`.

**`DeployedFlowState`** (`df.yaml:3324`) — 12 values: `DEPLOYING`, `GOOD_HEALTH`,
`CONCERNING_HEALTH`, `BAD_HEALTH`, `IMPORTING_FLOW`, `STARTING_FLOW`, `STOPPING_FLOW`, `FLOW_STOPPED`,
`CHANGING_FLOW_VERSION`, `UPDATING`, `TERMINATING`, `UNKNOWN`.

**Read flow-level state from `DeployedFlowState`, not `DeploymentState`.** The flow-lifecycle values
are deprecated on the deployment enum and live on the flow enum — so for an in-flight operation like a
version change, the deployment state is not a usable signal. Both enums expose the same wrapper shape:
`{state, detailedState, message}` (`DeploymentStatus` `df.yaml:2783`, `DeployedFlowStatus`
`df.yaml:3340`). `detailedState` is a free-form string, not an enum — do not switch on it.

Beyond state, `describe-deployment` (`Deployment`, `df.yaml:2841`) gives you
`currentNodeCount`, `autoscalingEnabled`, `autoscaleMinNodes` / `autoscaleMaxNodes`, `clusterSize`,
`nifiUrl`, `deployedByName`, and the three alert counters `activeErrorAlertCount`,
`activeWarningAlertCount`, `activeInfoAlertCount`. `describe-flow-in-deployment` (`DeployedFlow`,
`df.yaml:3356`) gives the same three counters at flow scope, plus `flowVersion`, `flowVersionCrn`,
`flowCrn` and `validActions`.

The alert counters are the cheapest health signal in the API: three integers, no metric window, no
pagination. **Poll those, and only fetch charts when one is non-zero.**

Note that `flowVersion`, `flowCrn` and `flowVersionCrn` on `Deployment` are all marked deprecated in
favour of the same fields on `DeployedFlow`. Read them from the flow.

---

## Configuring a KPI

Since per-component metrics exist only where a KPI does, configuring one is part of monitoring. Every
step here is **workload-plane, so VPC-only**.

Because `kpis` is a whole-array replace, this is a four-step read-modify-write and **not** a single
call. Skipping step 2 is how you delete KPIs by accident:

1. **Discover** — `get-flow-configuration-metadata-in-deployment` → the metric catalogue
   (`metricTypes[]`) and the component inventory (`contextGroups[]`).
2. **Read current state** — `get-flow-configuration-in-deployment` →
   `deployedFlowConfiguration` (`RpcDeployedFlowConfiguration`, `dfworkload.yaml:5204`), which carries
   the required **`configurationVersion`** *and* the existing **`kpis[]`**. There is no add or remove
   operation, so you need this array to preserve it.
3. **Build the full desired array** — existing entries plus yours.
4. **Write** — `update-flow-in-deployment` (`UpdateFlowInDeploymentRequest`, `dfworkload.yaml:6399`):

```bash
cdp dfworkload update-flow-in-deployment \
  --environment-crn       "$ENV_CRN" \
  --deployment-crn        "$DEPLOYMENT_CRN" \
  --deployed-flow-crn     "$DEPLOYED_FLOW_CRN" \
  --configuration-version "$CONFIG_VERSION" \
  --kpis '[...]'
```

Each entry is a `ConfiguredKpi` (`dfworkload.yaml:2442`, scope enum at `2456`):

| Field | Required | Notes |
|---|---|---|
| `metricId` | **yes** | An `id` from `metricTypes[]` |
| `metricComponentType` | no | One of the five scopes |
| `componentId` | no | Spec: *"the optional process group ID, processor ID, or connection ID. This is a composite ID containing a chain of process group IDs representing the component's full ancestry"* |
| `alert` | no | `ConfiguredAlert` — `thresholdMoreThan` / `thresholdLessThan` (each `{unitId, value}`) and a `frequencyTolerance` (`{unit, value}`) |
| `id` | no | Only when editing an existing KPI |

**`componentId` is a composite ancestry chain, not a bare UUID.** That is why you need the component
inventory before you can target a processor: the ID encodes the path to it, not just the component.

There is a validation rule stated only in prose, so no schema validator will catch it for you:
**at least one of `thresholdMoreThan` and `thresholdLessThan` is required** (`dfworkload.yaml:2402`).
Both are typed optional. `unitId` on each threshold comes from the catalogue's `defaultUnitId`, or
from `KpiMetaData.unitTypes` if you want a different one.

> ### `--kpis` is a whole-array replace, everywhere it appears
>
> Pass it and the KPIs you pass **replace all existing KPIs**. Omit it and existing KPIs are preserved.
>
> **Never pass `--kpis` to the deployment-level `cdp dfworkload update-deployment` on a shared tenant.** It is
> a whole-array replace with no undo at deployment scope, so it can silently erase a colleague's KPIs.
> When KPIs must change, use the narrower `update-flow-in-deployment` above.

### A 200 does not mean the alert is live

`RpcDeployedFlowConfiguration` carries a read-only **`kpisDirty`** flag, documented as *"whether or
not the current KPIs have successfully been deployed as alert rules."* Its existence tells you that
accepting the configuration and deploying the alert rule are **two separate events**.

**So a successful `update-flow-in-deployment` is not proof your KPI is evaluating.** Re-read
`get-flow-configuration-in-deployment` and check `kpisDirty` is false before you trust that an alert
will fire. `parametersDirty` is the same signal for parameter values, and `lastUpdatedByUsername` tells
you who touched the configuration last — useful on a shared tenant, given the whole-array hazard above.

---

## Response keys that silently return nothing

Guess one of these wrong and you get an empty result, not an error — which reads exactly like "the
flow has no data." Measured 2026-10-05.

| Call | Key |
|---|---|
| `list-flows-in-deployment` | `deployedFlows` — and it carries **no** `flowVersion` |
| `describe-flow-in-deployment` | `deployedFlow` → `.flowVersion` (int), `.flowVersionCrn` |
| `list-flow-kpis-in-deployment` | `metricCharts` at **top level** — not under `kpis` |
| `list-flow-definition-versions` | `flowVersions` |
| `get-flow-version` | `flowDefinition` — **base64**. `flowContents` is a key of the *decoded* JSON, not of the response |
| `get-flow-configuration-in-deployment` | `deployedFlowConfiguration` → `.kpis`, `.configurationVersion`, `.kpisDirty` |
| `describe-deployment` | `deployment` |

`cdp` has **no `--query`** flag, unlike the `az` and `aws` CLIs. Pipe the JSON through `jq`.

---

## What is not supported

Not available anywhere in `df` or `dfworkload`:

- **Any per-component metric for a component with no configured KPI.** This is the central limitation.
- **Arbitrary time ranges on metrics.** Four fixed windows, and they always return 25 buckets. (Events
  are the exception — they take `timestampFrom`.)
- **A raw metric scrape through `df` / `dfworkload`.** No endpoint in either API returns "all metrics
  for this flow." The Prometheus endpoint below does, which is why it is worth the trouble.
- **Adding or removing a single KPI.** `kpis` is whole-array replace; there is no add/remove operation.
- **Reading the metric catalogue from outside the VPC.** The call is workload-plane, and there is no
  spec enum to substitute for it.
- **`componentId` on `MetricChart`** — only `componentName`, and only via event detail for the ID.
- **Processor run-status reads or writes.** No start/stop/enable of an individual processor.
- **The bulletin board.** No access to NiFi bulletins.
- **Provenance.** No lineage or provenance query.
- **Queue listing or FlowFile inspection.** You can read a connection's queue *metric* if a KPI exists;
  you cannot list or peek at its contents.
- **`deleteFlowVersion`.** There is no such operation — catalogue versions accumulate permanently, and
  `delete-flow` is all-or-nothing. Every test import leaves a version behind forever.

### The NiFi REST API is not an escape hatch on DFX

The obvious workaround — call `/nifi-api` directly for the granularity the control plane lacks — does
not work headlessly on DFX Public Cloud. Four routes probed 2026-10-05, four failures:

| Route | Result |
|---|---|
| CDP workload token as bearer | Rejected. The token is **RS256**; NiFi's JWT verifier demands **EdDSA** |
| `/access/token` with username + password | HTTP **409**, "not supported" — no login identity provider configured |
| SAML2 | Browser-only redirect. No non-interactive grant |
| mTLS client certificate | The endpoint *requests* a cert but trusts only the **Let's Encrypt public root** — TLS terminates at a proxy and NiFi never sees the cert |

**One misleading non-finding, so nobody chases it:** `GET /nifi/` returns **200 unauthenticated**.
That is the static single-page app served before auth. Only `/nifi-api/*` is protected, and it is
closed.

Two workarounds exist and both were rejected as unviable for an application: copying a JWT out of a
browser session (short-lived, manual), and adding a `handleHttpRequest` processor per deployment as an
in-flow gateway (requires mutating every deployment, and only works for flows that already take
inbound connections).

**Scope note: Data Hub NiFi is different.** There, Knox fronts NiFi and ordinary CDP auth works
against the REST API. Everything in this section is about **DFX Public Cloud** specifically. Do not
carry a Data Hub habit over.

---

## Filling the gaps

Two routes to things the APIs above will not give you.

### Per-component metrics with no KPI: the Prometheus endpoint

DFX exposes a Prometheus federation endpoint per deployment:

```
https://<dfx-gateway>/dfx-<deployment-name>-ns/federate
```

HTTP Basic auth, user **`nifi-metrics`**.

**This is per-processor and per-connection for everything in the flow, with no KPI configuration
required** — precisely the granularity the `df` API lacks. If you need component-level numbers for
components nobody declared a KPI against, this is the documented way to get them.

> **Unverified, on two counts.** First, whether this endpoint is control-plane-reachable or VPC-only
> has not been established — check it against [Two APIs, two networks](#two-apis-two-networks) before
> assuming a laptop can reach it, and expect the timeout-not-refusal signature if it turns out to be
> VPC-bound. Second, **where the `nifi-metrics` password comes from is not established either.** The
> username is documented; the credential's source is not. Do not budget this as a solved auth path
> until both are checked.

### Component inventory and IDs: the flow definition

```bash
cdp df get-flow-version --flow-version-crn "$FLOW_VERSION_CRN" \
  | jq -r '.flowDefinition' | base64 --decode | jq '.flowContents'
```

**Mind the two layers.** `GetFlowVersionResponse` (`df.yaml:2414`) has exactly **one** property —
`flowDefinition`, `type: string`, `format: byte`, i.e. **base64**. `flowContents` is a top-level key
of the *decoded* JSON, not a field of the API response. Pipe `jq '.flowContents'` straight at the
response and you get `null`, which reads like an empty flow.

Once decoded, that JSON enumerates processors, process groups and connections with their IDs and their
nesting.

This is **design-time inventory, not runtime metrics** — it tells you what exists and how it is
arranged, never how it is behaving. Its value here is resolving the composite ancestry `componentId`
that a `ConfiguredKpi` needs, from a control-plane call that works off a laptop. The live alternative,
`contextGroups[]`, needs the VPC.

---

## What NOT to do

- **Don't difference a KPI value across two responses.** The buckets are anchored to request time, so
  you are comparing two grids. Measured error: ~20%. Difference inside one response only.
- **Don't widen the window without revisiting your staleness limit.** At `LAST_ONE_DAY` the newest
  bucket can be an hour old, and a short freshness check will then reject every sample and report
  `UNKNOWN` forever without erroring.
- **Don't pass `--kpis` to `dfworkload update-deployment` on a shared tenant.** Whole-array replace, no undo,
  deployment scope. Use `update-flow-in-deployment`.
- **Don't send `kpis` as a delta.** Read `get-flow-configuration-in-deployment` first and send the
  existing array plus your addition. Anything you omit is deleted.
- **Don't treat a successful `update-flow-in-deployment` as proof the alert is live.** Check
  `kpisDirty`.
- **Don't join a metric to a NiFi component on `componentName`** if you have any alternative. It is a
  display string with no uniqueness guarantee — two processors can share a name. Cache the mapping
  from `ConfiguredKpi.componentId` at configuration time instead.
- **Don't read a `cdp dfworkload` timeout as a credential problem.** If a workload-token expiry
  printed before the hang, your credentials and roles are fine and only the network path is missing.
- **Don't plan on the NiFi REST API for DFX monitoring.** All four auth routes are closed. Knox
  fronting is a Data Hub property, not a DFX one.
- **Don't treat an empty `metricCharts` array as a broken call.** It usually means no KPI is configured
  for that scope.
- **Don't GET-then-PUT a NiFi processor that has sensitive properties**, if you ever do reach a NiFi
  API (Data Hub, or an inbound connection). NiFi masks sensitive values as `********` on read; PUT that
  back and the literal overwrites and destroys the real credential.

---

## Security notes

- The Prometheus endpoint uses **HTTP Basic** with a fixed username, `nifi-metrics`. That makes the
  password a long-lived shared secret rather than a rotating token — if you end up using it, scope it,
  keep it out of the repo, and never put it in a flow definition or a job argument that lands in logs.
- Metric and alert payloads can carry **component names chosen by whoever built the flow**. Those are
  not guaranteed to be free of sensitive strings; do not pipe them into a prompt or a public dashboard
  without looking.
- All mutation paths here run on the workload plane, which means inside the VPC — so the thing holding
  your credentials is a long-lived in-VPC workload. Scope its CDP role to the DataFlow permissions it
  actually needs rather than reusing an admin key.
- `cdp df get-flow-version` returns the **whole flow definition**, and the spec marks that field
  **`x-skip-logging: true`** — the API's own authors consider it unfit for logs. It can carry endpoint
  hostnames, bucket paths and parameter names. Reading it is reasonable; echoing the decoded JSON into
  a log, a paste, or an LLM prompt is not.
- Every read command in this doc is non-mutating. `update-flow-in-deployment` is the only mutation
  here, and it **can delete KPIs** — exercise it against a non-production deployment first.

---

## References

- [Cloudera DataFlow service API](https://cloudera.github.io/cdp-dev-docs/api-docs/df/index.html) · [DataFlow Workload service API](https://cloudera.github.io/cdp-dev-docs/api-docs/dfworkload/index.html) — **both HTML pages truncate before the KPI and metric schemas. The YAML bundled with `cdpcli` is the complete source; prefer it.**
- [Monitoring flow deployments](https://docs.cloudera.com/dataflow/cloud/monitor-flow-deployments.html) · [Working with KPIs](https://docs.cloudera.com/dataflow/cloud/monitor-flow-deployments/topics/cdf-kpis-and-alerts.html)
- [DataFlow Prometheus metrics](https://docs.cloudera.com/dataflow/cloud/monitor-flow-deployments/topics/cdf-prometheus-metrics.html)
- [CDP CLI for DataFlow](https://docs.cloudera.com/dataflow/cloud/cli-reference.html)
- Sibling docs: [`flink-agents-on-cdf-azure.md`](./flink-agents-on-cdf-azure.md) — wiring agents to DataFlow · [`README.md`](./README.md)
