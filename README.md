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

For a single run of all four parts with the real output — one agent, Designer to YARN container, plus the
`yarn` and REST commands that prove it executed — see [Worked example:
`threshold_monitor`](#worked-example-threshold_monitor-designer-to-yarn) at the end.

---

### Part 1. Get the agents project working locally

Do this first even if the Data Hub is your real target. It is the only place you can iterate on an
agent in seconds instead of minutes, and it gives you a known-good baseline: if an agent fails on
YARN but works here, the problem is the deployment, not the agent.

Read that baseline narrowly, though. The local image runs **Flink 2.1.3**; a CSA 1.18 Data Hub runs
**Flink 1.20.5**. The Flink Agents API is the same 0.3 in both, so a local pass really does establish
that your agent's *logic* is sound — and establishes nothing whatsoever about the deployment. That is
also why there are two cluster runner scripts rather than one redundant pair.

```bash
git clone https://github.com/BrooksIan/FlinkDockerWithAgents.git
cd FlinkDockerWithAgents
pip install -e .
cp .env.example .env
```

`pip install -e .` installs the `ratatoskr` CLI, a Typer app. The `.env` copy is optional; it sets
`FLINK_REST_PORT=8082` among others.

Build the image and start the cluster:

```bash
ratatoskr build
ratatoskr up
```

`ratatoskr build` builds `agent_flink_image` from `deploy/Dockerfile`, which clones
`apache/flink-agents` at `release-0.3` and installs its wheel plus PyFlink **into the image** — that is
why `flink_agents` is importable in the container and not on your host. `ratatoskr up` is
`docker compose -f deploy/docker-compose.yml up -d` with profile `minimal`, i.e. jobmanager +
taskmanager.

Confirm it came up, and submit the simplest agent:

```bash
ratatoskr status
ratatoskr agent list
ratatoskr doctor

ratatoskr agent submit workflow_counter
```

`agent list` reads `examples/agents/agent-manifest.yaml`, so an agent missing from that file is invisible
to the CLI however well the module itself imports.

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

#### Carrying a Designer-authored agent to CSA

If you did author in the Designer, the agent needs one more step before Part 3 — and **nothing warns
you**, in either direction. What the Designer writes is not a module but a *shim*:
`examples/agents/published_shims/<name>.py` computes `parents[3]` and loads the real class from
`.ratatoskr/agents/<def_id>/agent.py`. Two independent consequences:

- **The bundler deletes it deliberately.** `build_csa_bundle.sh` does `rm -rf .../published_shims` when
  assembling `agentcode.zip`, so the agent never reaches the cluster at all.
- **Even shipped, it could not import.** `.ratatoskr/` is local Designer state — gitignored, and absent
  on every YARN node. The generated `cluster_import.py` sibling falls back to `/opt/flink/.ratatoskr/…`,
  which is the *Docker* image layout, not YARN's. Both branches raise at module-import time.

The fix is a copy, because generated `agent.py` files import only `flink_agents.api.*` and are
self-contained:

```bash
cp .ratatoskr/agents/<def_id>/agent.py examples/agents/my_agent.py
```

Find `<def_id>` in the shim's `_DEFINITION_ID`. That turns it into an ordinary module, which
`build_csa_bundle.sh` then picks up with everything else in `examples/agents/*.py`. Import the class by
its real name — the Designer suffixes it, so `my_test_agent` yields `MyTestAgentAgent` — then write the
cluster entry script as in Part 4. Confirm it actually made it — this must print a line; silence means
the bundle does not contain your agent:

```bash
unzip -l dist/csa/agentcode.zip | grep my_agent
```

Being a copy rather than a link, it is now a fork: later Designer edits do **not** reach the cluster
copy. Re-copy after each change, or treat the flattened module as the source of truth from then on.

### Part 3. Build the runtime on the existing CSA Data Hub cluster

This is the part that is genuinely hard, and it is **per cluster, not per agent** — do it once, then
Part 4 is cheap and repeatable. Full detail in the runbook; the shape of it:

> **Run every command in this Part on your own machine — not inside the Flink containers.** Worth
> stating because Parts 1 and 2 are the opposite: there, the runtime work happens *in* the jobmanager
> container. Three prompts are in play from here on:
>
> | Where | What runs there |
> |---|---|
> | **your machine (host)** | All of Part 3: `docker build` via `build_csa_bundle.sh`, every `scp`/`ssh`, and the Designer itself (`ratatoskr api` is a host uvicorn process, which is why `.ratatoskr/` is at the repo root) |
> | **the gateway** | `probe_csa_gateway.sh`, `probe_csa_workers.sh`, `build_csa_venv_gateway.sh`, and Part 4's submit |
> | **the jobmanager container** | Nothing in Part 3. This is Part 1's `ratatoskr agent submit` path |
>
> `deploy/docker-compose.yml` declares **no volume mounts and no Docker socket**, so the containers
> cannot see your repo (`ratatoskr agent submit` copies files in), cannot invoke `docker build`, and hold
> neither your SSH key nor the `cdp` CLI. `dist/csa/` must exist on the host regardless — it is what you
> `scp` from.
>
> The one thing that genuinely **cannot** run on the host is `import flink_agents` / `import pyflink`:
> that wheel is installed only into `agent_flink_image`, never into your virtualenv (see Part 1). So
> verify agent *code* in-container with `ratatoskr agent submit`, and build and ship the bundle from the
> host. Mixing those up is the most common way to lose an hour here.

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

**Then probe the workers**, because the gateway is not where the job runs — and ssh from the gateway to
the workers is refused, so the only way in is a YARN container. The `DRY_RUN=1` pass prints the plan
without submitting anything:

```bash
scp datahub/scripts/probe_csa_workers.sh <os-user>@<gateway>:~/
ssh <os-user>@<gateway>
kinit <workload-user>
DRY_RUN=1 bash ~/probe_csa_workers.sh
bash ~/probe_csa_workers.sh
```

This is the **one script here that is not read-only**: it submits a short distributed-shell application
(one container per worker, ~30–60 s, self-terminating), so confirm with the cluster owner. It prints a
per-node verdict and exits non-zero unless every worker reports `READY`. It matters because the next step
deliberately stops shipping a Python interpreter with the job and relies on the one already installed on
each node — if that is missing on even one worker, jobs fail *intermittently*, only when a TaskManager
lands there. [Step 0b](./flink-agents-on-csa-datahub.md#step-0b-prove-the-workers-not-just-the-gateway).

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

Ship the wheel and the venv script, and **only** those. `dist/csa/` also contains `agentenv.tar.gz`, the
superseded 650 MB conda archive — `scp -r` of the whole directory is the reflex to avoid. The `mkdir`
matters: without it, `scp` of individual files into a directory that does not exist yet fails.

```bash
ssh <os-user>@<gateway> 'mkdir -p ~/ratatoskr-csa'
scp dist/csa/wheel/flink_agents-*.whl datahub/scripts/build_csa_venv_gateway.sh \
    <os-user>@<gateway>:~/ratatoskr-csa/
ssh <os-user>@<gateway>
cd ~/ratatoskr-csa && chmod +x build_csa_venv_gateway.sh && ./build_csa_venv_gateway.sh
```

This needs outbound internet on the gateway for a handful of light dependencies; `pyflink` and `pemja`
come from the node's `/usr/local`, not from PyPI, which is the whole point of
`--system-site-packages`.

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

`SKIP_DOCKER=1` rebuilds only `agentcode.zip` — seconds, not the tens of minutes the Maven and conda
build takes.

```bash
SKIP_DOCKER=1 datahub/scripts/build_csa_bundle.sh
cp examples/agents/run_my_agent_cluster_csa.py dist/csa/
```

**3. Ship only what changed.** Do **not** `scp -r dist/csa/`. That directory also holds
`agentenv.tar.gz`, ~650 MB of conda environment that *nothing* reads any more — it used to reach the
TaskManagers via `-pyarch` and now ships nowhere (`submit_agent_csa.sh` explains this under "Why the
archive is gone"). Three files change between submits, and the wheel is needed only the first time on a
given gateway, by `build_csa_venv_gateway.sh`:

```bash
scp dist/csa/agentcode.zip dist/csa/run_my_agent_cluster_csa.py \
    dist/csa/submit_agent_csa.sh <os-user>@<gateway>:~/ratatoskr-csa/
```

**4. Inspect the command, then submit.** `chmod` because plain `scp` does not reliably carry the exec
bit. The `DRY_RUN=1` pass prints the `flink run` invocation without executing it and needs no Kerberos
ticket, so run it before `kinit`:

```bash
ssh <os-user>@<gateway>
cd ~/ratatoskr-csa
chmod +x submit_agent_csa.sh
ENTRY=run_my_agent_cluster_csa.py DRY_RUN=1 ./submit_agent_csa.sh

kinit <workload-user>
ENTRY=run_my_agent_cluster_csa.py ./submit_agent_csa.sh
```

`DRY_RUN=1` is worth using every time. The script runs 18 preflight checks and prints the exact
`flink run` invocation without executing it, which is where you catch a wrong `ENTRY` or a stale zip.

One trace in the submit output is benign and looks alarming: an `IllegalStateException: Trying to
access closed classloader` with `ShutdownHookManager` in the stack, printed *after* `Job has been
submitted`. That is Hadoop tearing down the **client** JVM. The job is unaffected.

Most of the submit script's inputs are auto-detected rather than defaulted, which matters when you
read its source: `FLINK_HOME`, `FLINK_CONF_DIR` (from `/etc/flink/conf`, then
`/etc/flink/conf.cloudera.flink`) and `HADOOP_CLASSPATH` (from `$(hadoop classpath)`) have **no
fallback value** — if detection fails the script exits rather than guessing. For a long-running job
set `KEYTAB` and `PRINCIPAL` together instead of relying on the `kinit` ticket cache. Every variable
is tabulated in the [Script reference](./flink-agents-on-csa-datahub.md#script-reference).

**5. Verify — see it for yourself.** Take the `applicationId` from the submit output. Everything below
runs on the gateway with a live ticket.

*Never search for the job by name.* `JOB_NAME` in the submit script is cosmetic, used only for the
script's own logging, and `yarn.application.name` is **ignored** in `yarn-per-job` mode:
`YarnClusterDescriptor.deployJobCluster` hardcodes the YARN application name to `"Flink per-job
cluster"`. Grepping for your agent's name will always come up empty — a genuinely confusing ten
minutes if you don't know it. Pass `-appStates ALL` too, or a bounded job that has already finished
will not appear, because the default listing shows only `RUNNING`/`ACCEPTED`/`SUBMITTED`:

```bash
yarn application -list -appStates ALL | grep 'Flink per-job cluster'
yarn application -status <applicationId>
```

`-status` gives you `State`, `Final-State`, and — the useful part — `Tracking-URL`.

**The Flink REST API survives the job, and this is the nicest way to prove a run.** The reasoning that
says otherwise is seductive and wrong: a per-job cluster's JobManager *is* the YARN application master,
so you would expect its REST API to die with the application, leaving only log greps. On CSA it does
not, because the parcel runs a **Flink History Server** on the gateway
(`org.apache.flink.runtime.webmonitor.history.HistoryServer`), and YARN's `Tracking-URL` for a
*finished* application points at it rather than at the dead AM. Note the host and port move: the AM ran
on a worker, but the tracking URL is the manager node at `historyserver.web.port`.

Two settings in `/etc/flink/conf/flink-conf.yaml` decide how you call it, and guessing either one wastes
time. Check them rather than assuming:

```bash
grep historyserver /etc/flink/conf/flink-conf.yaml
```

On CSA 1.18.0.0 they are `historyserver.web.ssl.enabled: true` and
`historyserver.security.spnego.auth.enabled: false`. So it is **https, and it does not want SPNEGO** —
the exact opposite of the reflex. Calling it over plain `http` fails as `curl: (52) Empty reply from
server`, which reads like a dead port rather than a TLS mismatch. `-k` skips certificate verification,
which is fine for a read-only look at your own job and not something to put in a script:

```bash
HS=$(yarn application -status <applicationId> 2>/dev/null | sed -n 's/.*Tracking-URL : //p' | tr -d ' ')
curl -sk "$HS/jobs/overview" | python3 -m json.tool
curl -sk "$HS/jobs/<jobId>" | python3 -m json.tool
```

`jobs/overview` lists every archived job with its `jid`, `name`, `state`, `duration` and task counts.
The `name` is your `agents_env.execute("…")` string — it *is* honoured, which is worth holding next to
the YARN application name that isn't. `jobs/<jid>` then gives the vertices, and this is the part worth
reading closely, because it quantifies the run instead of merely asserting it. For the stock threshold
agent:

| Vertex | Status | `write-records` | `read-records` |
|---|---|---|---|
| `Source: Collection Source -> _stream_key_by_map_operator` | FINISHED | 3 | 0 |
| `action-execute-operator -> Map, Map -> Sink: Print to Std. Out` | FINISHED | 0 | 3 |

Three records left the source and three entered the agent operator. A deployment that submitted
successfully but never ran your code shows the operator present with `read-records: 0`. Pair it with
the exceptions endpoint, where `root-exception: null` and an empty `all-exceptions` is what a clean run
looks like:

```bash
curl -sk "$HS/jobs/<jobId>/exceptions" | python3 -m json.tool
```

For a job that has already finished, the logs are the only record. Grep your **agent's** name, not a
field name — the output shape is whatever your module emits:

```bash
yarn logs -applicationId <applicationId> | grep threshold_monitor
```

A `FINISHED`/`SUCCEEDED` application proves less than it appears to: it says the client submitted
something and the cluster ran it to completion, not that *your* agent's code executed. Two greps give
you that:

```bash
yarn logs -applicationId <applicationId> | grep 'taskmanager.Task' | grep 'INITIALIZING to RUNNING'
yarn logs -applicationId <applicationId> | grep _output_event
```

Note what the first one is *not*: a bare `grep action-execute-operator` is the obvious thing to reach
for and it is too loose. It also matches the JobManager's `ExecutionGraph` lines, and those only prove
the JM *planned* the operator — a job that never obtained a slot still logs `CREATED to SCHEDULED`.
Narrowing to `taskmanager.Task` isolates the worker's own log, and `INITIALIZING to RUNNING` is the
transition that happens *after* the Python environment is built and pemja is loaded. Reaching `RUNNING`
there means the JNI bridge came up and your `@action` is executing as a Python UDF on a worker. No
client-side check can establish this — `pemja_core` cannot even be imported outside a JVM.

That grep returns **two** lines, one per vertex: the collection source and then
`action-execute-operator -> Map, Map -> Sink: Print to Std. Out`. Only the second is the proof you
want — the source is plain Java and would reach `RUNNING` even if the Python side were broken.

`_output_event` records are Flink Agents' own structured output, carrying the `jobId` and the
`taskName`, which ties each emitted value to a specific execution rather than to a `print()` that could
have come from anywhere. There are matching `_input_event` records, so you can read the whole
input → output pairing for every element:

```bash
yarn logs -applicationId <applicationId> | grep -E '_input_event|_output_event'
```

One practical note: `yarn logs` and `yarn application` bury their output in INFO chatter — a `YARN_OPTS`
deprecation warning and fifteen-plus `RangerRESTClient.init()` lines per invocation. Most of that is on
stderr, so `2>/dev/null` removes it. Not all: a couple of Ranger lines are *inside* the aggregated
container logs, because the TaskManager logged them itself, and no redirection will touch those. They
are harmless.

Because a green exit status is weak evidence, **design the test data so a wrong answer is visible** —
inputs that straddle a threshold, rather than a single value. The stock runner's
`[{"key": str(i), "value": i * 5} for i in range(1, 4)]` feeds 5/10/15 through a ×3 scale against a
threshold of 20, so a correct run must print `OK`, `ALERT`, `ALERT` in that order. A run that prints
three of the same thing ran *something*, but not this.

While an application is still `RUNNING`, log aggregation retains only the live container, so a
crash-looped app shows one attempt. Kill it first, then aggregate, to see them all.

Locally (Part 1) the same questions are much easier, because the Docker JobManager's REST port is
mapped and unauthenticated:

```bash
curl -s http://localhost:8082/jobs/overview | python3 -m json.tool
curl -s http://localhost:8082/taskmanagers | python3 -m json.tool
```

**If the JobManager enters a crash loop with nothing in the logs naming Python, agents, or a jar**,
read [The JobManager crash loop with no visible
cause](./flink-agents-on-csa-datahub.md#the-jobmanager-crash-loop-with-no-visible-cause) — the agents
dist jar bundles an AWS SDK v2 that shadows the platform's and breaks Ranger RAZ's S3 signer. The fix
is a classpath-ordering flag, and the symptom points nowhere near the cause.

And the flag syntax, one more time, because it costs more time than anything else here: **`-yD`, not
`-D`.** On CSA every `-D` is silently discarded.

---

## Worked example: `threshold_monitor`, Designer to YARN

Everything above is the general procedure. This section is one specific run of it, end to end, with the
real output — done on 2026-09-30 against a CSA 1.18.0.0 Data Hub (Flink 1.20.5) in CDP Public Cloud. It
exists so you can compare your own output to something known-good, and so the claims elsewhere in this
README are attributable to a run rather than to reasoning.

### Read this first: what the example does and does not do

**`threshold_monitor` monitors nothing.** It is a deployment smoke test wearing monitoring vocabulary,
and the distinction matters enough to state before the walkthrough rather than after. Its input is three
integers hardcoded in the runner script. There is no NiFi flow, no Kafka topic, no external source of
any kind — so when it emits `status: ALERT`, that is arithmetic on a constant, not a report about a
system.

That is the correct design for the job it has. The question being answered is *"did my Python agent code
execute on a YARN TaskManager with the pemja JNI bridge loaded"*, and any real data source would add
failure modes that muddy the answer. The threshold exists purely to make a **wrong** answer visible:
three records straddling a boundary produce a distinctive `OK, ALERT, ALERT` signature, so a run that
silently executed *different* code, or mangled the payload, prints something recognisably other. A
single input value could not do that. Falsifiability, not surveillance.

### The agent

Authored in the Designer, then flattened to a plain module (see Part 2 for why the flattening is
mandatory rather than stylistic). `examples/agents/threshold_monitor.py` in full is stdlib plus
`flink_agents.api.*` and nothing else, which is what lets the identical file load under Flink 2.1.3
locally and 1.20.5 on the cluster:

```python
SCALE = 3
THRESHOLD = 20

class ThresholdMonitorAgent(Agent):
    @tool
    @staticmethod
    def scale(value: int) -> int:
        return value * SCALE

    @action(InputEvent.EVENT_TYPE)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        reading = _int_from_input(event)
        scaled = ThresholdMonitorAgent.scale(reading)
        status = "ALERT" if scaled > THRESHOLD else "OK"
        ctx.send_event(OutputEvent(output={...}))
```

The runner supplies the input, and this one line is the whole data source:

```python
records = [{"key": str(i), "value": i * 5} for i in range(1, 4)]
```

`5, 10, 15` → scaled `15, 30, 45` → `OK, ALERT, ALERT`. Two alerts, zero systems observed.

### How it got from a laptop to a YARN container

```
  LAPTOP                                    GATEWAY                      WORKER
  ──────                                    ───────                      ──────
  Designer
     │ publish → shim in published_shims/
     │          (shim is STRIPPED at build; flatten or you ship nothing)
     ▼
  examples/agents/threshold_monitor.py
     │
     │ build_csa_bundle.sh
     │   ├─ docker build (linux/amd64) ─→ wheel + 2 dist jars
     │   └─ zip ratatoskr/ + examples/ ─→ agentcode.zip
     ▼
  dist/csa/
     │
     │ scp  (3 files per submit; wheel once per gateway)
     ▼
                                     ~/ratatoskr-csa/
                                       agentcode.zip
                                       run_workflow_cluster_csa.py
                                       submit_agent_csa.sh
                                       agentvenv/   ← built HERE, from the
                                                      wheel + the node's own
                                                      PyFlink (13 MB, not 626)
                                            │
                                            │ submit_agent_csa.sh
                                            │  18 preflight checks, then:
                                            │  flink run -t yarn-per-job
                                            │    -pyfs   agentcode.zip  ─────────┐
                                            │    -py     run_..._csa.py          │
                                            │    -pyexec /usr/bin/python3.11 ──┐ │
                                            │    -pyclientexec agentvenv/...   │ │
                                            ▼                                  │ │
                                       YARN app                                │ │
                                    "Flink per-job cluster"                    │ │
                                                                               ▼ ▼
                                                                     TaskManager JVM
                                                                       pemja → CPython
                                                                       runs @action
```

Two asymmetries in that picture are the whole trick. **`-pyexec` is the node's own
`/usr/bin/python3.11`**, not the venv — the venv (`-pyclientexec`) is a *client-side* interpreter only.
And **nothing Python-ish reaches the workers except `agentcode.zip`**; the 656 MB `agentenv.tar.gz` the
build still produces is vestigial and ships nowhere. Both facts are why the gateway venv is built with
`--system-site-packages`: `pyflink` and `pemja` must resolve to the *parcel's* versions, because those
are what the TaskManagers will use.

The submit script is where the accumulated scar tissue lives. It auto-detects `FLINK_HOME`,
`FLINK_CONF_DIR` and `HADOOP_CLASSPATH` with **no fallback values** — if detection fails it exits rather
than guessing — then runs 18 preflight checks before building the `flink run` invocation. `DRY_RUN=1`
stops after printing that invocation and needs no Kerberos ticket, so it is the cheapest way to catch a
wrong `ENTRY` or a stale zip.

### The commands, and the actual output

The submission, from `~/ratatoskr-csa` on the gateway:

```bash
DRY_RUN=1 ./submit_agent_csa.sh
kinit <workload-user>
./submit_agent_csa.sh
```

That yielded `application_1790640057726_0018`, whose AM landed on `worker2`. Finding it afterwards —
note `-appStates ALL`, since a bounded job has already finished, and note that the grep is for the
hardcoded name, never the agent's:

```bash
yarn application -list -appStates ALL | grep 'Flink per-job cluster'
yarn application -status application_1790640057726_0018
```

```
Application-Id  : application_1790640057726_0018
Application-Name : Flink per-job cluster          ← not the job name. Always this.
State : FINISHED    Final-State : SUCCEEDED    Progress : 100%
AM Host : pdf-pwc-worker2...   RPC Port : 32775
Tracking-URL : http://pdf-pwc-manager0...:18211   ← manager node, not the AM
Log Aggregation Status : SUCCEEDED
Aggregate Resource Allocation : 139962 MB-seconds, 44 vcore-seconds
```

**The agent's own output.** Grep your agent's name, not a field name — the shape is whatever your module
emits:

```bash
yarn logs -applicationId application_1790640057726_0018 2>/dev/null | grep threshold_monitor
```

```
{'reading': 5,  'scaled': 15, 'threshold': 20, 'status': 'OK',    'agent': 'threshold_monitor'}
{'reading': 10, 'scaled': 30, 'threshold': 20, 'status': 'ALERT', 'agent': 'threshold_monitor'}
{'reading': 15, 'scaled': 45, 'threshold': 20, 'status': 'ALERT', 'agent': 'threshold_monitor'}
```

`OK, ALERT, ALERT` in that order is the signature described above. Any other arrangement means something
other than this code ran.

**Proof the Python actually executed on a worker**, which the three lines above do *not* establish on
their own — a `print()` looks the same wherever it came from:

```bash
yarn logs -applicationId application_1790640057726_0018 2>/dev/null \
  | grep 'taskmanager.Task' | grep 'action-execute-operator.*INITIALIZING to RUNNING'
```

```
2026-09-30 03:15:11,061 INFO org.apache.flink.runtime.taskmanager.Task [] -
  action-execute-operator -> Map, Map -> Sink: Print to Std. Out (1/1)#0
  switched from INITIALIZING to RUNNING.
```

`taskmanager.Task` means it is the worker's own log, not the JobManager's plan, and `INITIALIZING to
RUNNING` is the transition that happens *after* the Python environment is built and pemja is loaded.

**Input/output pairing**, from Flink Agents' structured events — each record carries the `jobId` and
`taskName`, so values are tied to one execution:

```bash
yarn logs -applicationId application_1790640057726_0018 2>/dev/null | grep -E '_input_event|_output_event'
```

```
"eventType":"_input_event",  "jobId":"7d3beda9...","attributes":{"input":{"key":"1","value":5}}
"eventType":"_output_event", "jobId":"7d3beda9...","attributes":{"output":{"reading":5,"scaled":15,...,"status":"OK"}}
"eventType":"_input_event",  "jobId":"7d3beda9...","attributes":{"input":{"key":"2","value":10}}
"eventType":"_output_event", "jobId":"7d3beda9...","attributes":{"output":{"reading":10,"scaled":30,...,"status":"ALERT"}}
```

Incidentally, those `_input_event` records are also how you prove the *provenance* point at the top of
this section: the inputs are `5, 10, 15`, the list comprehension, arriving from nowhere.

**The durable view.** Per Part 4, the Flink History Server outlives the application:

```bash
HS=https://pdf-pwc-manager0...:18211
curl -sk "$HS/jobs/overview" | python3 -m json.tool
curl -sk "$HS/jobs/7d3beda905f51ee1bdfc7dde65a57c10" | python3 -m json.tool
```

```
jid      : 7d3beda905f51ee1bdfc7dde65a57c10
name     : Ratatoskr Threshold Monitor (CSA)    ← execute() name IS honoured here
state    : FINISHED     duration : 15739 ms     tasks: 2 total / 2 finished / 0 failed

Source: Collection Source -> _stream_key_by_map_operator   write-records 3, read-records 0
action-execute-operator -> Map, Map -> Sink: Print to Std. Out   read-records 3, write-records 0

/exceptions →  root-exception: null,  all-exceptions: []
```

`read-records: 3` on the agent vertex is the strongest single number here. An agent that was deployed
but never fed — the failure mode the stripped-`published_shims` trap produces — shows the vertex present
with `read-records: 0`, and a green `SUCCEEDED` beside it.

### Next: pointing an agent at a real NiFi flow

The obvious follow-on is an agent that watches a live NiFi flow in CDF Data Service in the same CDP
environment. The agents project already ships one — `examples/agents/workflow_nifi_monitor.py`
(`NiFiMonitorAgent`) with `run_workflow_nifi_monitor_cluster.py`, which supports a Kafka source, a
periodic tick, or N polls. Its architecture is worth noting because it is *not* the obvious one: the
Flink stream is a **clock**, and the agent calls the NiFi REST API itself from inside its tools, via
`ratatoskr.nifi.client.NiFiClient`.

**One blocker to clear first, and it fails in the worst possible way.** `build_csa_bundle.sh` stages an
explicit allowlist of `ratatoskr` submodules into `agentcode.zip` — `constants`, `paths`, `flink_rest`,
`kafka_sources`, plus `runtime/` and part of `agents/`. **`ratatoskr/nifi/` is not on that list**, so it
ships zero files. And because `workflow_nifi_monitor.py` imports `ratatoskr.nifi.client` *inside* its
tool functions rather than at module top level, nothing fails at submit time: the job goes green, reaches
a worker, and dies with `ModuleNotFoundError: No module named 'ratatoskr.nifi'` at action time. Same
shape as the flattening trap — a deployment that looks successful while running code that cannot work.

So the sequence is: add `ratatoskr/nifi/` to the bundle's staging list, confirm it appears in
`unzip -l dist/csa/agentcode.zip`, then work out NiFi endpoint reachability and auth from a YARN
container — which is a genuinely separate problem, since workers reach neither the gateway's
`~/.kube`-style local config nor anything that inherits your interactive Kerberos ticket. Also note that
a Kafka-sourced or tick-driven job is **unbounded**, so it stays `RUNNING`: the live JobManager REST API
becomes reachable through the YARN proxy and the History Server stops being the only window.

---

## Repo map

| Path | What it is |
|---|---|
| [`flink-agents-on-csa-datahub.md`](./flink-agents-on-csa-datahub.md) | Data Hub runbook: AWS + CDP infra from scratch, build, submit, every trap found |
| [`flink-agents-on-cdf-azure.md`](./flink-agents-on-cdf-azure.md) | Operator runbook: Azure + AKS infra from scratch, image build, `FlinkDeployment`, DataFlow and AI Inference integration |
| [`datahub/scripts/probe_csa_gateway.sh`](./datahub/scripts/probe_csa_gateway.sh) | Go/no-go probes. **Run this first** — it can kill the approach in ten minutes |
| [`datahub/scripts/probe_csa_workers.sh`](./datahub/scripts/probe_csa_workers.sh) | Verifies the PyFlink runtime on **every worker**, inside a real YARN container. The only script here that submits an application |
| [`datahub/scripts/build_csa_bundle.sh`](./datahub/scripts/build_csa_bundle.sh) | Builds the Flink Agents jars + wheel (local, `linux/amd64`) |
| [`datahub/scripts/build_csa_venv_gateway.sh`](./datahub/scripts/build_csa_venv_gateway.sh) | Builds the Python env **on the gateway**, against the node's own PyFlink |
| [`datahub/scripts/submit_agent_csa.sh`](./datahub/scripts/submit_agent_csa.sh) | The submit path. `DRY_RUN=1` prints the command without submitting |
| [`datahub/deploy/Dockerfile.csa-build`](./datahub/deploy/Dockerfile.csa-build) | Build-only image for the 1.20 jars and wheel |
| [`datahub/examples/run_workflow_cluster_csa.py`](./datahub/examples/run_workflow_cluster_csa.py) | The example job — `workflow_counter`, no Kafka, no LLM |
| [`datahub/patches/`](./datahub/patches/) | Runtime portability patch against the source agents project |

Full usage for each script — every environment variable, default, and what it checks — is in the
[Script reference](./flink-agents-on-csa-datahub.md#script-reference) section of the Data Hub runbook.

> **`datahub/scripts/` in THIS repo is canonical.** To run them you copy them into a clone of the
> agents project (they live at `scripts/` there, and each resolves `REPO_ROOT` as
> `$(dirname "$BASH_SOURCE")/..`, so they work from either location without edits). That copy is a
> **snapshot, not a link** — edit it and the fix never reaches here.
>
> This has already bitten once. On 2026-09-30 the two copies had drifted by 6–13 lines each, and the
> clone's `build_csa_bundle.sh` was missing `docker cp "$CID:/out/wheel" "$OUT_DIR/"` — so a full
> rebuild from it would silently produce a bundle with no wheel, and you would only find out on the
> gateway when `build_csa_venv_gateway.sh` reports "no wheel found", after the scp. Fix here, then
> re-copy; never the other direction.

## Status and provenance

| | |
|---|---|
| Data Hub path | **Verified end to end** 2026-09-29 — CSA 1.18.0.0, Flink 1.20.5, Runtime 7.3.2, Java 17, RAZ-enabled, S3. `workflow_counter` FINISHED/SUCCEEDED in one AM attempt |
| Operator path | Written from Cloudera's documented behaviour and the CSA Operator chart. **Not** verified end to end in this repo — treat the commands as a starting point, not a tested script |
| Flink Agents | `release-0.3` / `0.3.1`, built from source. `0.3-SNAPSHOT` artifacts, no release guarantee |
| Agents source project | [`BrooksIan/FlinkDockerWithAgents`](https://github.com/BrooksIan/FlinkDockerWithAgents) ("ratatoskr") — the agents themselves, and the local Docker path they were developed on |

Neither path is a Cloudera-supported configuration for Python agent workloads. Read the
"what is not supported" section of whichever runbook you follow before committing to it.
