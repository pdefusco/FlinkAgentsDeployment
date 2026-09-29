# Flink Agents on Cloudera

Running [Apache Flink Agents](https://github.com/apache/flink-agents) on Cloudera, on both of the
deployment models Cloudera offers for Flink — with the infrastructure build-out, the submit path, and
the traps, for each.

- **[flink-agents-on-csa-datahub.md](./flink-agents-on-csa-datahub.md)** — CSA on CDP Public Cloud
  **Data Hub** (VMs, YARN). Verified end to end on AWS.
- **[flink-agents-on-cdf-azure.md](./flink-agents-on-cdf-azure.md)** — **CSA Operator** on your own
  Kubernetes (AKS), wired to Cloudera DataFlow.
- **[datahub/](./datahub/)** — the working scripts and job for the Data Hub path.

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
