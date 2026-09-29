# Flink Agents on Cloudera

Running [Apache Flink Agents](https://github.com/apache/flink-agents) on Cloudera, on both of the
deployment models Cloudera offers for Flink — with the infrastructure build-out, the submit path, and
the traps, for each.

- **[flink-agents-on-csa-datahub.md](./flink-agents-on-csa-datahub.md)** — CSA on CDP Public Cloud
  **Data Hub** (VMs, YARN). Verified end to end on AWS.
- **[flink-agents-on-cdf-azure.md](./flink-agents-on-cdf-azure.md)** — **CSA Operator** on your own
  Kubernetes (AKS), wired to Cloudera DataFlow.
- **[datahub/](./datahub/)** — the working scripts and job for the Data Hub path.

**New to this?** Start with the [walkthrough](#walkthrough-from-the-agents-project-to-a-running-agent-on-csa)
— author an agent locally, then get it onto an existing Data Hub cluster, in four parts.

---

## Why this repo exists

Flink Agents is a new (0.3.x, pre-release) framework for building agentic applications on Flink: an
agent is a Flink operator, so it inherits Flink's state, checkpointing, exactly-once semantics and
horizontal scale rather than reinventing them. That makes Flink an unusually good substrate for
agents that must react to streams continuously rather than answer one prompt at a time.

Cloudera is an obvious place to run that, because the streams are usually already there — NiFi flows,
Kafka topics, a Data Lake. But **Cloudera has two entirely different Flink deployment models**, they
have almost nothing operationally in common, and Flink Agents is not a first-class citizen in either.
There is no official Flink Agents image, no Cloudera integration, and on the Data Hub path PyFlink
itself is unsupported.

So the practical question — *"can I run Flink Agents on the Cloudera cluster I already have, and
what will it cost me?"* — has no documented answer. This repo is that answer, arrived at empirically,
including the parts that do not work and the dead ends that looked like they should.

---

## The two Cloudera Flink deployment models

### 1. CSA Operator for Kubernetes

Cloudera's distribution of the Apache Flink Kubernetes Operator, plus Cloudera components such as SQL
Stream Builder. You bring the Kubernetes cluster; the operator is the control plane for Flink
application lifecycle.

```
Your Kubernetes cluster (AKS / EKS)
│
├── CSA Operator  ── watches FlinkDeployment / FlinkSessionJob CRDs
│      │
│      ├──────────────────┬──────────────────┐
│      ▼                  ▼                  ▼
│  ┌────────────┐   ┌────────────┐    SQL Stream Builder
│  │ JobManager │   │ JobManager │    (+ its PostgreSQL)
│  │    Pod     │   │    Pod     │
│  └─────┬──────┘   └─────┬──────┘
│        │                │
│   ┌────┴────┐      ┌────┴────┐
│   ▼         ▼      ▼         ▼
│ TaskMgr  TaskMgr  TaskMgr  TaskMgr     ← pods
│
└── Kubernetes schedules and scales all of it
```

Installing the operator does **not** create a Flink cluster. It registers the CRDs and runs the
operator and its admission webhook. You then declare a cluster and a job as Kubernetes resources:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
spec:
  image: <your-registry>/flink-agents:0.3.1     # ← you control the image
  flinkVersion: v1_20
  serviceAccount: flink                          # ← the webhook requires this
  flinkConfiguration:
    classloader.parent-first-patterns.additional: pemja
  jobManager: { resource: { memory: 1536m, cpu: 1 } }
  taskManager: { resource: { memory: 1536m, cpu: 1 } }
```

The operator reconciles that desired state. Because **you build the image**, every Python
dependency — including pemja, the JNI bridge Flink Agents needs — is baked in and version-matched at
build time. That is the single biggest practical advantage of this model for agent workloads.

### 2. CSA on CDP Public Cloud Data Hub

A **Streaming Analytics** Data Hub: cloud VMs running the full Cloudera Runtime stack, with Flink
alongside YARN, ZooKeeper, HDFS and Cloudera Manager. Cloudera provides Light Duty and Heavy Duty
cluster definitions (Heavy Duty is tuned for state-intensive workloads).

```
CDP Public Cloud Environment  (+ Data Lake, FreeIPA, Ranger/RAZ, Knox)
│
└── Streaming Analytics Data Hub
     │
     ├── Gateway node  ◀── you SSH here and run `flink run`
     ├── Master nodes  ── Cloudera Manager, YARN RM, HDFS NN, ZooKeeper
     └── Worker nodes  ── YARN NodeManagers
          │
          └── Flink on YARN (per-job or session)
               ├── JobManager   ← a YARN ApplicationMaster container
               └── TaskManagers ← YARN containers
```

Deployment is traditional. Cloudera's own Flink tutorial is essentially:

```bash
mvn clean package
scp my-flink-job.jar user@<gateway>:.
ssh user@<gateway>
flink run -d -p 2 my-flink-job.jar
```

The job is then visible in the Flink Dashboard and the History Server exposed through the
environment. Here the runtime is **Cloudera's parcel, which you cannot modify** — so shipping Python
dependencies becomes the hard problem, and it is where nearly all the difficulty on this path lives.

### Side by side

| | CSA Operator (Kubernetes) | CSA Data Hub (VMs) |
|---|---|---|
| Infrastructure | Your AKS/EKS cluster | CDP Data Hub VMs |
| Flink orchestration | Flink Kubernetes Operator | YARN |
| Desired-state controller | Kubernetes operator | YARN + Cloudera Manager |
| Deployment unit | Kubernetes resource + container image | Job submitted with `flink run` |
| JobManager | Pod | YARN ApplicationMaster container |
| TaskManagers | Pods | YARN containers |
| Deployment interface | `kubectl`, CRDs, GitOps | `ssh` + `flink run`, CM ecosystem |
| **Flink version** | **You pin it** (`flink:1.20.5-java17`) | Whatever the CSA parcel ships |
| **Runtime image** | **Yours** | Cloudera's parcel — not modifiable |
| **Python dependencies** | Baked into the image | Shipped per-submission via `-pyfs`/`-pyarch` |
| HDFS / YARN / ZooKeeper | Not part of the architecture | Included in the cluster definition |
| Scaling | Kubernetes | Data Hub resize |
| SQL Stream Builder | Supported | Supported |
| Cloudera management depth | Operator integration | Deep CM / CDP integration |

### What that means specifically for Flink Agents

The rows above are generic Flink. These are the ones that decided this project:

| | CSA Operator | CSA Data Hub |
|---|---|---|
| Cloudera support for **PyFlink** | Supported deployment model | **Unsupported** — "Virtual environments for Python" is on the CSA unsupported-features list |
| Getting **pemja** version-matched | Pin it in the Dockerfile | Must match the parcel's `flink-python` jar exactly — and the pin moves **within** a Flink minor (1.20.1 → pemja 0.4.1, 1.20.5 → pemja 0.5.7) |
| Shipping agent code | `COPY` into the image | `-pyfs` zip, with a strict flat-layout rule |
| Config overrides | `flinkConfiguration:` in the CRD | `-yD` — and **`-D` is silently discarded** |
| Classpath conflicts | You control the whole image | The agents jar's bundled AWS SDK collides with Ranger RAZ |
| Verified in this repo | Documented, **not** end-to-end verified | **Verified**: FINISHED/SUCCEEDED on CSA 1.18.0.0 / Flink 1.20.5 |

---

## Which one should you use?

**Use the CSA Operator** if you have any choice in the matter. You pin the Flink version, you bake the
Python environment into an image you control, and PyFlink is inside Cloudera's supported model. For
anything heading toward production, this is the route.

**Use the Data Hub path** when the Data Hub is the substrate you have been given — it is already
provisioned, it already has your Kafka and your Data Lake, and standing up Kubernetes alongside it is
not on the table. It works. It is also unsupported for PyFlink and it depends on CSA implementation
details that an upgrade can change, so re-verify after every upgrade.

**Hard requirement for the Data Hub path:** you need **SSH access to the gateway node**. Without it
there is no submit path at all, and the question is settled for you.

If you are on the Data Hub path and short on time, read [the one finding that matters
most](./flink-agents-on-csa-datahub.md#the-one-finding-that-matters-most) before anything else: **on
CSA, `flink run -D<key>=<value>` is silently discarded for every key — use `-yD`.** No warning, no
error. It was the single largest time sink in this work.

---

## Walkthrough: from the agents project to a running agent on CSA

The runbooks are organised by *problem* — here is the same material organised as a **sequence**, for
someone starting with nothing and ending with their own agent running on a Data Hub cluster.

This assumes the **Data Hub cluster already exists**. If it does not, build it first with
[Provisioning the infrastructure from
scratch](./flink-agents-on-csa-datahub.md#provisioning-the-infrastructure-from-scratch), then come
back here.

| | | Where it runs |
|---|---|---|
| **Part 1** | Get the agents project working locally | Your laptop, Docker |
| **Part 2** | Author a new Flink Agent | Your laptop |
| **Part 3** | Build the runtime on the CSA cluster | Laptop, then the gateway. **Once per cluster** |
| **Part 4** | Deploy the agent as a script | The gateway |

Parts 1–2 are the [`BrooksIan/FlinkDockerWithAgents`](https://github.com/BrooksIan/FlinkDockerWithAgents)
("ratatoskr") project as its author designed it — nothing in this repo changes it. Parts 3–4 are this
repo's addition.

---

### Part 1. Get the agents project working locally

Do this first even if the Data Hub is your real target. It is the only place you can iterate on an
agent in seconds instead of minutes, and it gives you a known-good baseline: if an agent fails on
YARN but works here, the problem is the deployment, not the agent.

```bash
git clone https://github.com/BrooksIan/FlinkDockerWithAgents.git
cd FlinkDockerWithAgents
pip install -e .            # installs the `ratatoskr` CLI (a Typer app)
cp .env.example .env         # optional; sets FLINK_REST_PORT=8082 among others
```

Build the image and start the cluster:

```bash
ratatoskr build              # builds agent_flink_image from deploy/Dockerfile:
                             #   clones apache/flink-agents at release-0.3 and installs
                             #   its wheel + PyFlink INTO THE IMAGE
ratatoskr up                 # docker compose -f deploy/docker-compose.yml up -d
                             #   profile `minimal` = jobmanager + taskmanager
```

Confirm it came up, and submit the simplest agent:

```bash
ratatoskr status
ratatoskr agent list         # reads examples/agents/agent-manifest.yaml
ratatoskr doctor

ratatoskr agent submit workflow_counter
```

`agent submit` copies the agent module, its cluster script, and the supporting `ratatoskr/` runtime
into the `jobmanager` container, then runs `flink run -py <script>` inside it. The job appears at
**<http://localhost:8082>** (the `minimal` profile maps `${FLINK_REST_PORT:-8082}:8081`; the `full`
honeypot profile is a different compose file and uses 8081 — don't mix them up).

Two things about local runs worth knowing before you hit them:

- **`ratatoskr agent run <name> --local` runs on your *host* Python**, not in Docker. But
  `flink_agents` is only installed into the container image — it is not a dependency in
  `pyproject.toml`, and nothing in the project installs it into your virtualenv. On a fresh clone
  `import flink_agents` will fail on the host. Prefer `ratatoskr agent submit` (in-container) unless
  you have separately built and installed the `flink_agents` wheel yourself.
- **Only agents with a `cluster_script:` in the manifest can be submitted at all.** About seven of
  them have one. Everything else is local-only. See Part 2.

### Part 2. Author a new Flink Agent

There is **no `ratatoskr agent create` / scaffold command.** Agents are hand-written, and the
established pattern in the project is to copy an existing example and adjust it. Four files, two of
them edits:

**1. The agent — `examples/agents/my_agent.py`.** This is the whole of `workflow_counter`, which is
the right thing to copy: a `@tool`, an `@action`, no Kafka and no LLM.

```python
from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext


class CounterAgent(Agent):
    """Workflow agent that doubles integer values from input events."""

    @tool
    @staticmethod
    def double(value: int) -> int:
        """Return twice the input value."""
        return value * 2

    @action(InputEvent.EVENT_TYPE)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        payload = InputEvent.from_event(event).input
        n = int(payload.get("value", 0) if isinstance(payload, dict) else payload)
        result = CounterAgent.double(n)
        ctx.send_event(
            OutputEvent(output={"input": n, "doubled": result, "agent": "workflow_counter"})
        )
```

Note `@action(InputEvent.EVENT_TYPE)` — a **string**, not the event class. In Flink Agents 0.3+
actions listen on event *type* strings, and passing the class silently never fires. Both decorators
sit above `@staticmethod`, in that order.

**2. A local runner — `examples/agents/run_my_agent_local.py`.** Copy `run_workflow_local.py`: it
imports `AgentsExecutionEnvironment`, does `env.from_list(data).apply(MyAgent()).to_list()`, then
`env.execute()`.

**3. A cluster script — `examples/agents/run_my_agent_cluster.py`.** Copy
`run_workflow_cluster.py`. **This file is what makes the agent submittable at all**; without it,
`ratatoskr agent submit` raises `ValueError: Agent 'my_agent' has no cluster script configured`.

**4. Register it — `examples/agents/agent-manifest.yaml`.** Add a top-level key. This is the runtime
registry the CLI reads:

```yaml
my_agent:
  type: workflow                                    # workflow | react
  description: One-line summary
  entry: examples.agents.my_agent:MyAgent           # must be module:ClassName
  runner: examples/agents/run_my_agent_local.py
  cluster_script: examples/agents/run_my_agent_cluster.py   # optional, but see above
```

Optionally also add an entry to `examples/agents/agent-catalog.yaml` with `input_schema` /
`output_schema` — that file drives the web dashboard and Studio, joined back to the manifest by
`manifest: my_agent`. The CLI does not read it.

Then verify locally before going anywhere near YARN:

```bash
ratatoskr agent list | grep my_agent
ratatoskr agent submit my_agent
```

> **The Agent Designer is a trap for this workflow.** The visual designer at `/designer` can generate
> an agent and will auto-register it in both YAML files — but the code that writes those entries
> **never emits `cluster_script`**. A Designer-published agent therefore cannot be submitted with
> `ratatoskr agent submit`, and cannot be carried onto CSA by Part 4 without hand-writing a cluster
> script for it first. Use the Designer to explore; hand-write what you intend to deploy.

### Part 3. Build the runtime on the existing CSA Data Hub cluster

This is the part that is genuinely hard, and it is **per cluster, not per agent** — do it once, then
Part 4 is cheap and repeatable. Full detail in the runbook; the shape of it:

> **Two different usernames are in play — this catches everyone once.**
>
> | | Who | Used for |
> |---|---|---|
> | `<os-user>` | The cloud OS account CDP creates — **`cloudbreak`** on AWS | `ssh` / `scp`, paired with the SSH key from environment creation |
> | `<workload-user>` | Your CDP **workload** username | `kinit`, and therefore your HDFS/YARN identity |
>
> You log in as the OS account and *then* `kinit` as your workload user. The Kerberos identity comes
> from the ticket, not from the OS login, so these do not have to match and normally don't. Getting
> the gateway FQDN: `cdp datahub describe-cluster --cluster-name <cluster>` — you want the node in
> the **`GATEWAY`** instance group, not a master. Use `ssh -A` if you need to reach worker nodes;
> they are only reachable by hopping through the gateway.

**Probe first.** This can end the exercise in ten minutes rather than after a day of building:

```bash
scp datahub/scripts/probe_csa_gateway.sh <os-user>@<gateway>:~/
ssh <os-user>@<gateway> 'bash ~/probe_csa_gateway.sh'
```

The decisive check is whether the Flink parcel ships `flink-python-*.jar` in `$FLINK_HOME/lib/`. If
it does not, PyFlink was stripped and there is no path — go to the Operator model.
[Step 0](./flink-agents-on-csa-datahub.md#step-0-probe-the-cluster-before-building-anything).

**Build the jars and wheel locally,** in a `linux/amd64` container (cluster nodes are x86_64; an
Apple Silicon Mac must cross-build or pemja's compiled extension is the wrong architecture):

```bash
cd <your clone of the agents project>
FLINK_MAJOR_MINOR=1.20 datahub/scripts/build_csa_bundle.sh
```

Output lands in `dist/csa/` — the `flink_agents` wheel, the two dist jars, `agentcode.zip`, the entry
script, and the submit script.
[Step 1](./flink-agents-on-csa-datahub.md#step-1-build-the-jars-and-the-wheel-once-locally).

**Build the Python environment on the gateway.** Not locally — and this is the single most useful
thing this repo found. CSA nodes already ship a complete, version-matched pip-installed PyFlink stack
(on CSA 1.18.0.0: `apache-flink` 1.20.5, `pemja` 0.5.7, plus `libpython3.11.so`). So the environment
is a thin `--system-site-packages` venv over the node's own stack, not a cross-built archive — **13 MB
instead of 626 MB**, and matched by construction rather than by luck:

```bash
scp -r dist/csa/ <os-user>@<gateway>:~/ratatoskr-csa/
scp datahub/scripts/build_csa_venv_gateway.sh <os-user>@<gateway>:~/ratatoskr-csa/
ssh <os-user>@<gateway>
cd ~/ratatoskr-csa && ./build_csa_venv_gateway.sh
```

[Step 2](./flink-agents-on-csa-datahub.md#step-2-build-the-python-environment-on-the-gateway).

> **Re-run this after any CSA upgrade, including a patch bump.** `apache-flink`'s `pemja` pin moves on
> nearly every 1.20.x patch release, into **non-overlapping** ranges — 1.20.1/1.20.2 → `pemja==0.4.1`,
> 1.20.3 → `0.5.5`, 1.20.4 → `0.5.6`, 1.20.5 → `0.5.7`. Matching the Flink *minor* is not sufficient;
> any patch mismatch is a guaranteed pemja mismatch, not merely a risk. The venv script's
> parcel-versus-wheel check is what catches it.

### Part 4. Deploy the agent as a script

With Part 3 done, deploying an agent is: write a CSA entry script, refresh the code zip, submit.

**1. Write the CSA entry script.** Copy
[`datahub/examples/run_workflow_cluster_csa.py`](./datahub/examples/run_workflow_cluster_csa.py) to
`examples/agents/run_my_agent_cluster_csa.py` and change one import and one pipeline. It differs from
the Docker cluster script in three deliberate ways, all of which you should keep:

| | Why |
|---|---|
| No `/opt/flink` on `sys.path` | `-pyfs agentcode.zip` puts the shipped modules on `PYTHONPATH` instead |
| No `patch_flink_agents_version()` | The Docker image has no real `apache-flink` dist metadata so it fabricates some. On CSA the metadata is real — fabricating it could only disagree with it |
| **Keeps** `patch_flink_agents_jar_loading()` | Forces both dist jars through a *single* `add_jars` call so pemja resolves to one class identity. Flink Agents' own loader adds them one at a time, which splits pemja across classloaders → `ClassCastException` on `pemja.core.object.PyObject` (FLINK-39226). This matters **more** on YARN, not less |

**2. Refresh the bundle.** `build_csa_bundle.sh` picks up every `examples/agents/*.py` into
`agentcode.zip` automatically, so your agent module travels with no further work. The **entry script
is copied by name**, though — the script has `cp examples/agents/run_workflow_cluster_csa.py` hardcoded
— so either add a line for yours or copy it across by hand:

```bash
SKIP_DOCKER=1 datahub/scripts/build_csa_bundle.sh    # rebuilds only agentcode.zip, seconds not minutes
cp examples/agents/run_my_agent_cluster_csa.py dist/csa/
scp -r dist/csa/ <os-user>@<gateway>:~/ratatoskr-csa/
```

**3. Inspect the command, then submit.**

```bash
ssh <os-user>@<gateway>
kinit <workload-user>
cd ~/ratatoskr-csa

ENTRY=run_my_agent_cluster_csa.py DRY_RUN=1 ./submit_agent_csa.sh   # print, don't submit
ENTRY=run_my_agent_cluster_csa.py ./submit_agent_csa.sh             # submit
```

`DRY_RUN=1` is worth using every time. The script runs 18 preflight checks and prints the exact
`flink run` invocation without executing it, which is where you catch a wrong `ENTRY` or a stale zip.

Most of the submit script's inputs are auto-detected rather than defaulted, which matters when you
read its source: `FLINK_HOME`, `FLINK_CONF_DIR` (from `/etc/flink/conf`, then
`/etc/flink/conf.cloudera.flink`) and `HADOOP_CLASSPATH` (from `$(hadoop classpath)`) have **no
fallback value** — if detection fails the script exits rather than guessing. For a long-running job
set `KEYTAB` and `PRINCIPAL` together instead of relying on the `kinit` ticket cache. Every variable
is tabulated in the [Script reference](./flink-agents-on-csa-datahub.md#script-reference).

**4. Verify.** Take the `applicationId` from the submit output — do **not** try to find the job by
name:

```bash
yarn logs -applicationId <applicationId> | grep doubled
```

`JOB_NAME` in the submit script is cosmetic, used only for the script's own logging.
`yarn.application.name` is **ignored** in `yarn-per-job` mode: `YarnClusterDescriptor.deployJobCluster`
hardcodes the YARN application name to `"Flink per-job cluster"`. Grepping `yarn application -list`
for your agent's name will always come up empty, and this is a genuinely confusing ten minutes if you
don't know it.

**If the JobManager enters a crash loop with nothing in the logs naming Python, agents, or a jar**,
read [The JobManager crash loop with no visible
cause](./flink-agents-on-csa-datahub.md#the-jobmanager-crash-loop-with-no-visible-cause) — the agents
dist jar bundles an AWS SDK v2 that shadows the platform's and breaks Ranger RAZ's S3 signer. The fix
is a classpath-ordering flag, and the symptom points nowhere near the cause.

And the flag syntax, one more time, because it costs more time than anything else here: **`-yD`, not
`-D`.** On CSA every `-D` is silently discarded.

---

## Repo map

| Path | What it is |
|---|---|
| [`flink-agents-on-csa-datahub.md`](./flink-agents-on-csa-datahub.md) | Data Hub runbook: AWS + CDP infra from scratch, build, submit, every trap found |
| [`flink-agents-on-cdf-azure.md`](./flink-agents-on-cdf-azure.md) | Operator runbook: Azure + AKS infra from scratch, image build, `FlinkDeployment`, DataFlow and AI Inference integration |
| [`datahub/scripts/probe_csa_gateway.sh`](./datahub/scripts/probe_csa_gateway.sh) | Go/no-go probes. **Run this first** — it can kill the approach in ten minutes |
| [`datahub/scripts/build_csa_bundle.sh`](./datahub/scripts/build_csa_bundle.sh) | Builds the Flink Agents jars + wheel (local, `linux/amd64`) |
| [`datahub/scripts/build_csa_venv_gateway.sh`](./datahub/scripts/build_csa_venv_gateway.sh) | Builds the Python env **on the gateway**, against the node's own PyFlink |
| [`datahub/scripts/submit_agent_csa.sh`](./datahub/scripts/submit_agent_csa.sh) | The submit path. `DRY_RUN=1` prints the command without submitting |
| [`datahub/deploy/Dockerfile.csa-build`](./datahub/deploy/Dockerfile.csa-build) | Build-only image for the 1.20 jars and wheel |
| [`datahub/examples/run_workflow_cluster_csa.py`](./datahub/examples/run_workflow_cluster_csa.py) | The example job — `workflow_counter`, no Kafka, no LLM |
| [`datahub/patches/`](./datahub/patches/) | Runtime portability patch against the source agents project |

Full usage for each script — every environment variable, default, and what it checks — is in the
[Script reference](./flink-agents-on-csa-datahub.md#script-reference) section of the Data Hub runbook.

## Status and provenance

| | |
|---|---|
| Data Hub path | **Verified end to end** 2026-09-29 — CSA 1.18.0.0, Flink 1.20.5, Runtime 7.3.2, Java 17, RAZ-enabled, S3. `workflow_counter` FINISHED/SUCCEEDED in one AM attempt |
| Operator path | Written from Cloudera's documented behaviour and the CSA Operator chart. **Not** verified end to end in this repo — treat the commands as a starting point, not a tested script |
| Flink Agents | `release-0.3` / `0.3.1`, built from source. `0.3-SNAPSHOT` artifacts, no release guarantee |
| Agents source project | [`BrooksIan/FlinkDockerWithAgents`](https://github.com/BrooksIan/FlinkDockerWithAgents) ("ratatoskr") — the agents themselves, and the local Docker path they were developed on |

Neither path is a Cloudera-supported configuration for Python agent workloads. Read the
"what is not supported" section of whichever runbook you follow before committing to it.
