# Deploying Cloudera Flink Agents Alongside Cloudera DataFlow on Azure (CDP Public Cloud 7.3.2)

This guide stands up **Apache Flink Agents** as Flink jobs on the **Cloudera Streaming Analytics (CSA) Operator**, running in your own AKS cluster next to your CDP Public Cloud environment on Azure, and wires those agents to the NiFi flows you already run in **Cloudera DataFlow (CDF)**. The agents observe your deployed flows and Kafka topics, reason over what they see with a model served by Cloudera AI Inference, and take corrective action behind an explicit approval gate. You leave with a running agents cluster, three working integration paths into CDF, and a set of gated Workflow and ReAct agents.

The design has two kinds of agents. **Workflow agents** are deterministic and rule-based. They watch a flow or a topic and raise a signal. **ReAct agents** reason with an LLM and propose a runbook, but never mutate a live system without a human approving the step first. Both run as ordinary Flink jobs on a session-mode cluster, so you submit and cancel them the same way you would any Flink job.

```
Cloudera DataFlow (CDP Public Cloud, Azure)          Your AKS cluster (same VNet)
┌───────────────────────────────┐                   ┌──────────────────────────────────┐
│  NiFi flow deployments         │ Inbound Conn mTLS │  CSA Operator                      │
│  Data Hub Kafka (9093)         │◀─────────────────▶│    └─ flink-agents FlinkDeployment │
│  DataFlow service (control     │  Kafka SASL_SSL   │         Workflow + ReAct agents    │
│  plane API)                    │  df / dfworkload  │         run here as Flink jobs     │
└───────────────────────────────┘                   │            │                       │
                                                     │            ▼                       │
                                                     │   Cloudera AI Inference (ReAct)    │
                                                     └──────────────────────────────────┘
```

---

## Prerequisites

- A **CDP Public Cloud environment on Azure**, Runtime **7.3.2**, with a **Cloudera DataFlow** service and at least one deployed NiFi flow.
- A **customer-managed AKS cluster** in the same Azure subscription, peered to the VNet your CDP environment runs in, so in-VNet hostnames and the Data Hub brokers are reachable.
- `kubectl` and `helm` (v3) configured against that AKS cluster, and an **Azure Container Registry (ACR)** the cluster can pull from.
- A **Cloudera license file** and credentials for `container.repository.cloudera.com` (the Cloudera image registry).
- A **CDP workload user** and **workload password** from the Management Console, used for the Kafka path.
- A **CDP access key pair** (access key ID + private key, Management Console → User Management), used by `cdp df` for deployment status and KPIs. This is a different credential from the workload password — the DataFlow APIs do not accept the latter.
- For ReAct agents, a **Cloudera AI Inference** endpoint serving a chat model, plus a Knox JWT or Knox API key to call it.

---

## Provisioning the infrastructure from scratch

Skip this section if the environment, AKS cluster and ACR in the prerequisites already exist — go
straight to [Step 1](#step-1-install-the-csa-operator-into-aks).

> **None of the commands in this section were run for this repo.** The operator path is documented
> from Cloudera's published behaviour and the CSA Operator Helm chart; only the [Data Hub
> path](./flink-agents-on-csa-datahub.md) was verified end to end. Azure CLI and CDP CLI flag sets
> also vary between versions — use `az <group> <cmd> --help` and
> `cdp <service> <cmd> --generate-cli-skeleton` to confirm against your own versions. **Treat
> everything below as a checklist of what must exist, not as verified copy-paste.**

### 1. Install and authenticate the CLIs

```bash
# Azure CLI
az login
az account set --subscription <subscription-id>
az account show

# CDP CLI
pip install cdpcli
cdp configure                      # access key + private key from Management Console → User Management
cdp iam get-user                   # confirm auth

# Set the workload password — the Kafka path uses it
# (the df/dfworkload APIs authenticate with the access key pair from `cdp configure`, not this)
cdp iam set-workload-password --password '<workload-password>'
```

### 2. Azure prerequisites for the CDP environment

CDP needs these to exist before it will register an Azure environment. Cloudera publishes a
`cdp-azure-prerequisites` ARM template that creates most of them in one step; doing it by hand means:

| What | Why |
|---|---|
| Resource group | Everything below lives in it |
| VNet with at least 3 subnets across availability zones | Data Lake and Data Hub node placement |
| Storage account (ADLS Gen2, **hierarchical namespace enabled**) with containers for data and logs | Data Lake storage and log collection. HNS is required, not optional |
| App registration (service principal) with **Contributor** on the resource group | How CDP provisions on your behalf |
| Managed identities for the Data Lake admin, log collection, Ranger audit, and IDBroker | Cloudera's mandatory identity set — these carry the role assignments onto the storage containers |
| SSH public key | Node access |

```bash
az group create --name <rg> --location <region>

az network vnet create \
  --resource-group <rg> --name <vnet> \
  --address-prefix 10.10.0.0/16 \
  --subnet-name cdp-subnet-1 --subnet-prefix 10.10.0.0/20

# add two more subnets in other zones
az network vnet subnet create --resource-group <rg> --vnet-name <vnet> \
  --name cdp-subnet-2 --address-prefix 10.10.16.0/20
az network vnet subnet create --resource-group <rg> --vnet-name <vnet> \
  --name cdp-subnet-3 --address-prefix 10.10.32.0/20

# ADLS Gen2 — hierarchical namespace is required
az storage account create \
  --resource-group <rg> --name <storageacct> \
  --sku Standard_LRS --kind StorageV2 --hns true
```

Then register the credential with CDP:

```bash
cdp environments create-azure-credential \
  --credential-name <cred-name> \
  --subscription-id <subscription-id> \
  --tenant-id <tenant-id> \
  --app-based applicationId=<app-id>,secretKey=<client-secret>

cdp environments list-credentials
```

### 3. Create the environment and Data Lake

```bash
cdp environments create-azure-environment \
  --environment-name <env-name> \
  --credential-name <cred-name> \
  --region <region> \
  --security-access cidr=<your-cidr> \
  --public-key '<ssh-public-key>' \
  --resource-group-name <rg> \
  --existing-network-params networkId=<vnet>,resourceGroupName=<rg>,subnetIds=cdp-subnet-1,cdp-subnet-2,cdp-subnet-3 \
  --log-storage storageLocationBase=abfs://logs@<storageacct>.dfs.core.windows.net,managedIdentity=<log-identity-id> \
  --use-public-ip

cdp environments describe-environment --environment-name <env-name>

cdp datalake create-azure-datalake \
  --datalake-name <datalake-name> \
  --environment-name <env-name> \
  --cloud-provider-configuration managedIdentity=<idbroker-identity-id>,storageLocation=abfs://data@<storageacct>.dfs.core.windows.net \
  --scale LIGHT_DUTY

cdp datalake describe-datalake --datalake-name <datalake-name>
cdp environments sync-all-users
```

Environment creation takes 20–40 minutes, most of it FreeIPA. The Data Lake is what brings up Ranger
and Knox, and Knox is what the AI Inference calls in Step 5 go through. The DataFlow calls in Step 4
do **not** use Knox — `cdp df` is a public CDP API authenticated with your access key pair.

### 4. Create the AKS cluster and ACR

This is the part that has no CDP equivalent — on this path **you own the Kubernetes cluster**. Put it
in the same VNet as the CDP environment if you can, or peer it; the agents need to reach the Data Hub
Kafka brokers over in-VNet hostnames, and the `cdp dfworkload` gateway resolves to a private
`internal-*` address that is only reachable from inside the VNet. See
[`cdf-monitoring-apis.md`](./cdf-monitoring-apis.md) for which monitoring calls need that reach and
which work from anywhere.

```bash
az acr create --resource-group <rg> --name <youracr> --sku Standard

# Option A — AKS in a subnet of the CDP VNet (no peering needed)
az network vnet subnet create --resource-group <rg> --vnet-name <vnet> \
  --name aks-subnet --address-prefix 10.10.48.0/20

az aks create \
  --resource-group <rg> --name <aks-name> \
  --node-count 3 --node-vm-size Standard_D4s_v3 \
  --network-plugin azure \
  --vnet-subnet-id "$(az network vnet subnet show --resource-group <rg> \
      --vnet-name <vnet> --name aks-subnet --query id -o tsv)" \
  --attach-acr <youracr> \
  --generate-ssh-keys

az aks get-credentials --resource-group <rg> --name <aks-name>
kubectl get nodes
```

`--attach-acr` grants the cluster's kubelet identity `AcrPull`, which is what lets the
`FlinkDeployment` in Step 3 pull your image without a pull secret of its own. (You still need the
`cloudera-creds` secret in Step 1 — that is for Cloudera's registry, not yours.)

If AKS is in a **separate** VNet, peer it in both directions:

```bash
az network vnet peering create --resource-group <rg> \
  --name aks-to-cdp --vnet-name <aks-vnet> \
  --remote-vnet "$(az network vnet show -g <rg> -n <vnet> --query id -o tsv)" \
  --allow-vnet-access

az network vnet peering create --resource-group <rg> \
  --name cdp-to-aks --vnet-name <vnet> \
  --remote-vnet "$(az network vnet show -g <rg> -n <aks-vnet> --query id -o tsv)" \
  --allow-vnet-access
```

A peering that exists in only one direction reports `Connected` on the side you created and still
drops traffic. Check both.

### 5. Create the DataFlow service and a Data Hub for Kafka

The agents in Step 4 read from Kafka and query the DataFlow APIs, so both need to exist:

```bash
# DataFlow (CDF) on the environment
# NB: `cdp` has no --query (unlike az/aws) — pipe its JSON through jq instead.
cdp df enable-service --environment-crn "$(cdp environments describe-environment \
  --environment-name <env-name> | jq -r '.environment.crn')" \
  --min-k8s-node-count 3 --max-k8s-node-count 5 --use-public-load-balancer

cdp df list-services

# A Streaming Data Hub for the Kafka brokers
cdp datahub list-cluster-definitions \
  | jq -r '.clusterDefinitions[].clusterDefinitionName' | grep -i streaming

cdp datahub create-azure-cluster \
  --cluster-name <kafka-cluster> \
  --environment-name <env-name> \
  --cluster-definition-name '<definition-from-above>'
```

Then deploy a NiFi flow through the DataFlow Catalog, and note its Inbound Connection endpoint — Step
4 needs it.

### 6. Confirm before building the image

```bash
kubectl get nodes                                    # AKS reachable
az acr login --name <youracr>                        # ACR push works
cdp datalake describe-datalake --datalake-name <datalake-name> | grep status
cdp df list-deployments                              # the NiFi flow is running
# Kafka broker reachability from inside the cluster:
kubectl run netcheck --rm -it --image=busybox --restart=Never -- \
  nc -zv <broker-host> 9093
```

That last check is the one worth not skipping. If a pod in AKS cannot open 9093 on a Data Hub broker,
the peering or the security group is wrong, and you will otherwise discover it several steps later as
a Flink job that starts cleanly and consumes nothing.

---

## Step 1. Install the CSA Operator into AKS

The CSA Operator ships as an OCI Helm chart. It installs the Flink Kubernetes Operator, which manages the `FlinkDeployment` and `FlinkSessionJob` custom resources, and the SQL Stream Builder services. Create a namespace for it and a pull secret for the Cloudera registry first.

```bash
kubectl create namespace flink-agents

kubectl create secret docker-registry cloudera-creds \
  --namespace flink-agents \
  --docker-server=container.repository.cloudera.com \
  --docker-username='<cloudera-registry-user>' \
  --docker-password='<cloudera-registry-token>'
```

Install the operator, passing your license file inline and the pull secret to each subchart.

```bash
helm install csa-operator \
  oci://container.repository.cloudera.com/cloudera-helm/csa-operator/csa-operator \
  --namespace flink-agents \
  --version 1.5.0-b275 \
  --set 'flink-kubernetes-operator.imagePullSecrets[0].name=cloudera-creds' \
  --set 'ssb.sse.image.imagePullSecrets[0].name=cloudera-creds' \
  --set 'ssb.sqlRunner.image.imagePullSecrets[0].name=cloudera-creds' \
  --set 'ssb.mve.image.imagePullSecrets[0].name=cloudera-creds' \
  --set 'ssb.database.imagePullSecrets[0].name=cloudera-creds' \
  --set 'ssb.flink.image.imagePullSecrets[0].name=cloudera-creds' \
  --set-file flink-kubernetes-operator.clouderaLicense.fileContent=./license.txt
```

Confirm the operator is up before going further.

```bash
kubectl get deploy -n flink-agents
kubectl get crd | grep flink.apache.org   # flinkdeployments, flinksessionjobs
```

The operator's admission webhook **requires** `spec.serviceAccount` on every `FlinkDeployment`. Step 3 creates that service account.

> **Durable state.** For anything beyond a demo, back SQL Stream Builder's metadata and the Flink checkpoints with persistent volumes rather than the defaults. Configure the persistence values in a `csa-values.yaml` and pass it with `--values` at install time. On AKS, use a managed-disk `storageClassName`.

---

## Step 2. Build the Flink Agents runtime image

There is no official Flink Agents image, so you build one. It is a multi-stage build. A Maven stage compiles Apache Flink Agents `release-0.3.1` from source (the Java dist jars plus a Python wheel), and a runtime stage layers those onto the official `flink:1.20.5-java17` image with an isolated Python virtualenv. This Flink version sits above the Flink Agents 1.20.3 floor and matches the 1.20 line the CSA Operator runs.

```dockerfile
# ---- Stage 1: build Flink Agents from source ----
FROM maven:3-eclipse-temurin-17 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        git python3 python3-venv python3-pip && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone --depth 1 --branch release-0.3.1 https://github.com/apache/flink-agents.git
WORKDIR /src/flink-agents
RUN mvn clean install -DskipTests -B -Dspotless.skip=true
RUN set -eux; \
    LIB=python/flink_agents/lib; rm -rf "$LIB"; mkdir -p "$LIB/common"; \
    cp dist/common/target/flink-agents-dist-common-*.jar "$LIB/common/"; \
    for d in dist/flink-*; do v=$(basename "$d"); mkdir -p "$LIB/$v"; \
        cp "$d"/target/flink-agents-dist-"$v"-*-thin.jar "$LIB/$v/"; done; \
    python3 -m venv /buildvenv && /buildvenv/bin/pip install --no-cache-dir build; \
    cd python && /buildvenv/bin/python -m build --wheel

# ---- Stage 2: runtime image ----
FROM flink:1.20.5-java17
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.11 python3.11-venv python3.11-dev build-essential && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/flink-agents/python/dist/*.whl /tmp/wheels/
RUN python3.11 -m venv /opt/flink/agents-venv \
    && /opt/flink/agents-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/flink/agents-venv/bin/pip install --no-cache-dir \
        apache-flink==1.20.5 kafka-python /tmp/wheels/*.whl && rm -rf /tmp/wheels
# Put the dist jars and the flink-python jar on the Flink classpath so PythonDriver runs.
COPY --from=build /src/flink-agents/dist/common/target/flink-agents-dist-common-*.jar /opt/flink/lib/
COPY --from=build /src/flink-agents/dist/flink-1.20/target/flink-agents-dist-flink-1.20-*-thin.jar /opt/flink/lib/
RUN cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/
# Your agent entry scripts.
RUN mkdir -p /opt/flink/usrlib/agents
COPY agents/ /opt/flink/usrlib/agents/
ENV PYFLINK_CLIENT_EXECUTABLE=/opt/flink/agents-venv/bin/python
ENV PYTHONPATH=/opt/flink/agents-venv/lib/python3.11/site-packages
RUN ln -sf /opt/flink/agents-venv/bin/python /usr/local/bin/python \
    && chown -R flink:flink /opt/flink/agents-venv /opt/flink/usrlib
USER flink
```

Two things this image gets right that are easy to miss. Flink Agents runs the Python agent code through `pemja` in embedded-libpython mode, so the virtualenv's site-packages has to be exposed on `PYTHONPATH`, because pemja does not pick up the venv on its own. And the `flink-python` jar has to be copied into `/opt/flink/lib/` so `PythonDriver` is on the classpath at submit time.

Build it and push it to your ACR.

```bash
az acr build --registry <your-acr> --image cso-flink-agents:0.3.1 .
```

---

## Step 3. Deploy the agents FlinkDeployment and RBAC

The agents run on a **session-mode** cluster, one long-lived Flink cluster that many small agent jobs are submitted to and cancelled from, which matches how agents come and go. First create the service account the admission webhook requires and the role the JobManager needs to manage its TaskManager pods.

```yaml
# rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: flink
  namespace: flink-agents
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: flink
  namespace: flink-agents
rules:
  - apiGroups: [""]
    resources: ["pods", "configmaps", "services", "secrets", "events"]
    verbs: ["create", "get", "list", "watch", "update", "delete", "patch"]
  - apiGroups: ["apps"]
    resources: ["deployments", "statefulsets"]
    verbs: ["create", "get", "list", "watch", "update", "delete", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: flink
  namespace: flink-agents
subjects:
  - kind: ServiceAccount
    name: flink
    namespace: flink-agents
roleRef:
  kind: Role
  name: flink
  apiGroup: rbac.authorization.k8s.io
```

Then the session cluster itself, pointed at the image in your ACR.

```yaml
# flinkdeployment.yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink-agents
  namespace: flink-agents
spec:
  image: <your-acr>.azurecr.io/cso-flink-agents:0.3.1
  imagePullPolicy: IfNotPresent
  flinkVersion: v1_20
  serviceAccount: flink
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    classloader.parent-first-patterns.additional: pemja
    python.executable: /opt/flink/agents-venv/bin/python
    python.client.executable: /opt/flink/agents-venv/bin/python
  jobManager:
    resource:
      memory: 1536m
      cpu: 1
  taskManager:
    resource:
      memory: 1536m
      cpu: 1
```

The `classloader.parent-first-patterns.additional: pemja` line is not optional. Without it, pemja's embedded libpython loads two copies of its classes and the TaskManager crashes.

```bash
kubectl apply -f rbac.yaml
kubectl apply -f flinkdeployment.yaml
kubectl get flinkdeployment -n flink-agents   # wait for STABLE
```

### Smoke-test with the simplest possible agent

Before wiring anything to Kafka or an LLM, prove that the image can run a Flink Agents job at all.
This is the same test as the Data Hub path's milestone, and for the same reason: if pemja's two halves
disagree or the dist jars are not on the classpath, **this** is where you want to find out, not three
integrations later.

`smoke_counter.py` — no Kafka, no LLM, no external state:

```python
from pyflink.datastream import StreamExecutionEnvironment
from flink_agents.api.execution_environment import AgentsExecutionEnvironment

from agents.workflow_counter import CounterAgent   # your agent

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
agents_env = AgentsExecutionEnvironment.get_execution_environment(env)

records = [{"key": str(i), "value": i * 5} for i in range(1, 4)]
stream = env.from_collection(records)
keyed = agents_env.from_datastream(input=stream, key_selector=lambda row: row["key"])
keyed.apply(CounterAgent()).to_datastream().print()
agents_env.execute("smoke counter")
```

Add it to the image alongside your agents (Step 2 already `COPY`s an agents directory into
`/opt/flink/usrlib/agents/`), then:

```bash
kubectl exec -it deploy/flink-agents -n flink-agents -- \
  flink run -py /opt/flink/usrlib/agents/smoke_counter.py

# the printed records land in the TaskManager log
kubectl logs -n flink-agents -l component=taskmanager --tail=50 | grep doubled
```

Three records in, three doubled records out, job `FINISHED`. Note what is **not** here that the Data
Hub path needs: no `-yD` flags, no `-pyfs` zip, no classpath ordering, no Kerberos. Everything those
compensate for was settled when you built the image — which is the whole argument for this path.

> The [Data Hub example](./datahub/examples/run_workflow_cluster_csa.py) is the same agent with a
> preflight wrapper around it, because there the environment is discovered at submit time rather than
> baked in. Comparing the two files is a quick way to see exactly what the operator path buys you.

---

## Step 4. Wire the agents to Cloudera DataFlow

There are three integration surfaces, and most deployments use more than one. Take them in the order below.

### Reaching the flow itself

- **DataFlow Inbound Connections.** A CDF flow deployment can expose a stable public hostname with TLS/mTLS auto-provisioned. The listen processor uses a `StandardRestrictedSSLContextService` named **exactly** `Inbound SSL Context Service`, which CDF auto-populates at deployment. This is the clean edge path when the agent needs to push data to the flow from outside the VNet.
- **The NiFi REST API is not available on DFX Public Cloud.** Four headless auth routes to `/nifi-api` were probed on a live deployment (2026-10-05) and all four are closed: the CDP workload token is RS256 where NiFi's verifier demands EdDSA; `/access/token` with user+password returns HTTP 409 "not supported"; SAML2 offers only a browser redirect; and mTLS terminates at a proxy that trusts only the Let's Encrypt public root. **Knox fronting is a Data Hub NiFi property, not a DFX one** — do not plan a monitoring loop around processor status, connection queues or the bulletin board. Those signals come from the DataFlow APIs instead; see [`cdf-monitoring-apis.md`](./cdf-monitoring-apis.md) for what they do and do not expose.

Keep the monitor phase on **read-only** calls. The two rules below apply wherever an agent does write to NiFi — on Data Hub, or through an Inbound Connection — and they are the reason the heal gate in Step 5 exists.

- **Never GET-then-PUT a NiFi processor that has sensitive properties.** NiFi masks a sensitive value as `********` on read. PUT it back and that literal overwrites the real credential and destroys it. Change run status through the narrow `/processors/{id}/run-status` endpoint, or manage the value in a **Parameter Context**, never a full-entity PUT.
- **Put credentials in a Parameter Context**, not in a processor property and not in the FlinkDeployment YAML.

### The data plane over Data Hub Kafka

When the agents need to consume the events a flow produces (or feed enriched results back), connect directly to the environment's **Data Hub Kafka** on port **9093** with these client settings.

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
# workload user / workload password via a JAAS config or client-supplied callback
```

Import the environment's FreeIPA certificate into the client truststore with `keytool`, and take the broker hostnames from Cloudera Manager for that cluster. Agents subscribe to the topic the flow writes and publish their enriched output to a topic of your choosing.

### Flow and deployment monitoring from the DataFlow APIs

With `/nifi-api` closed, this is the monitoring surface. It is more capable than "deployment is up or
down": the DataFlow APIs expose five KPI scope types, so **processor-, process-group- and
connection-level metrics are all available** —

```
SYSTEM · NIFI_FLOW · NIFI_PROCESSOR · NIFI_PROCESS_GROUP · NIFI_CONNECTION
```

Two constraints shape how an agent uses them:

- **Per-component metrics exist only where a KPI was configured.** There is no "read all processors"
  call in either API. `SYSTEM` and `NIFI_FLOW` metrics need no setup; the other three require a KPI
  targeting that specific component. Decide what to watch at design time, not at alert time.
- **Reads and writes sit on different networks.** `cdp df` is the public control plane and works from
  anywhere — so a laptop or an agent outside the VNet can read KPI values and deployment state.
  `cdp dfworkload` resolves to a private `internal-*` ELB, so the metric catalogue and every KPI
  mutation are VPC-bound. An agent that only reads can run anywhere; one that configures KPIs cannot.

```bash
cdp df list-flow-kpis-in-deployment \
  --deployment-crn "$DEPLOYMENT_CRN" \
  --deployed-flow-crn "$DEPLOYED_FLOW_CRN" \
  --metrics-time-period LAST_THIRTY_MINUTES | jq '.metricCharts'
```

Feed this in as a distinct agent input from the Kafka data plane. **[`cdf-monitoring-apis.md`](./cdf-monitoring-apis.md)
is the full reference** — the metric chart shape, the 25-bucket trap that makes cross-request
differencing wrong by ~20%, how to discover the metric catalogue, the KPI write path, and the
workarounds for what the APIs do not cover.

---

## Step 5. Point ReAct agents at Cloudera AI Inference

Flink Agents 0.3.1 does not ship a vendor-specific Cloudera integration, but Cloudera AI Inference exposes an **OpenAI-compatible** endpoint, and the SDK's `OPENAI_COMPLETIONS_CONNECTION` speaks exactly that. So a ReAct agent points at AI Inference with no code fork. You supply the endpoint, a token, and the model name.

The AI Inference endpoint follows the pattern `https://<domain>/namespaces/serving-default/endpoints/<endpoint-name>/v1`. The client `base_url` is that URL with the last two path segments removed. Auth is a bearer token, either a Knox JWT or a Knox API key. The `model` you pass must be the **name assigned in the AI Registry**, not a raw Hugging Face or NGC id.

```python
from flink_agents.api.resource import ResourceDescriptor, ResourceName

ai_inference_connection = ResourceDescriptor(
    clazz=ResourceName.ChatModel.OPENAI_COMPLETIONS_CONNECTION,
    api_base_url="https://<domain>/namespaces/serving-default/endpoints/<endpoint-name>/v1",
    api_key="<knox-jwt-or-api-key>",
)

# On the agent, the setup references the connection by name and names the registry model:
#   ResourceDescriptor(
#       clazz=ResourceName.ChatModel.OPENAI_COMPLETIONS_SETUP,
#       connection="ai_inference",
#       model="<ai-registry-model-name>",
#   )
```

The Knox JWT is short-lived. For an agent job that runs longer than the token's lifetime, use a **Knox API key** rather than a JWT so the job does not lose auth mid-run.

If you would rather run your own model, the same `OPENAI_COMPLETIONS_CONNECTION` works against any OpenAI-compatible endpoint. Point `api_base_url` at it and set `api_key` accordingly.

---

## Heal-phase guardrails

The agents move through three phases, and the gate between them is where policy is enforced in code, not just documented.

- **monitor** is read-only. Agents observe and raise signals, and nothing is changed.
- **safe** covers low-risk, reversible actions, each one approved by a human before it runs.
- **lab** covers broader actions, in a non-production flow only.

Every ReAct runbook is **human-in-the-loop**. The agent proposes the action and a person approves it before any mutation. Fold the NiFi rules from Step 4 into this gate. No GET-then-PUT of sensitive properties, credentials in a Parameter Context, and confirm before you restart or redeploy any live service. An approval you gave for one action does not carry to the next.

---

## Running on a CSA Data Hub instead of your own operator

If you would rather not run the operator yourself, provision a **Streaming Analytics Data Hub** in your CDP environment and submit the agent jobs to its Flink cluster on YARN instead of to a FlinkDeployment in your AKS. You trade operator-level control over the image, Flink version and sizing for a Cloudera-managed cluster.

**That path is written up in full in [`flink-agents-on-csa-datahub.md`](./flink-agents-on-csa-datahub.md)** — verified end to end on CSA 1.18.0.0 / Flink 1.20.5, with the build, the submit command, and the traps. Read it before you commit to the route, because two things there are decisive:

- **PyFlink on CSA is outside Cloudera support.** "Virtual environments for Python" is on the CSA unsupported-features list. The operator route above is the supported one.
- **You need SSH access to the Data Hub gateway.** Without it, there is no submit path.

The operator route lets you pin `flink:1.20.5-java17` directly and is supported, which is why it remains the recommended one.

---

## What NOT to do

- **Don't expose a Kafka broker directly when Inbound Connections will do.** The Inbound Connection gives you a public hostname with auto-provisioned mTLS and no broker exposure. Reach for the direct 9093 path only when a flow is not in the picture.
- **Don't plan on the NiFi REST API for DFX monitoring.** `/nifi-api` has no working headless auth route on DFX Public Cloud (four probed, four closed, 2026-10-05), and Knox does not front it the way it fronts Data Hub NiFi. Build the monitor phase on `cdp df` instead.
- **Don't assume a `cdp dfworkload` failure is a credential problem.** It is almost always the network — that gateway is VPC-only. The tell: a printed workload-token expiry *followed by* a hang is network; a failure with no expiry line is the credential case.
- **Don't GET-then-PUT a NiFi processor with sensitive properties.** The masked `********` writes back as a literal and destroys the credential. Use `/run-status` or a Parameter Context.
- **Don't hardcode credentials in the FlinkDeployment YAML.** Inject them as environment or secret references, and keep flow credentials in a NiFi Parameter Context.
- **Don't call Cloudera AI Inference "GA."** State it as available, and do not attach a maturity label it does not carry.

---

## References

- [Cloudera DataFlow Inbound Connections](https://docs.cloudera.com/dataflow/cloud/about-inbound-connections.html) · [Configuring inbound connection support](https://docs.cloudera.com/dataflow/cloud/develop-flow-definitions/topics/cdf-configuring-inbound-connection-support.html)
- DataFlow API reference: [`df` (control plane)](https://cloudera.github.io/cdp-dev-docs/api-docs/df/index.html) · [`dfworkload`](https://cloudera.github.io/cdp-dev-docs/api-docs/dfworkload/index.html). Both HTML pages **truncate before the KPI and metric schemas** — the OpenAPI YAML bundled with `cdpcli` (`cdpcli/data/df/df.yaml`, `cdpcli/data/dfworkload/dfworkload.yaml`) is the complete source. See [`cdf-monitoring-apis.md`](./cdf-monitoring-apis.md).
- [Connecting Kafka clients outside the VPC](https://docs.cloudera.com/cdf-datahub/7.3.1/connecting-kafka/topics/kafka-dh-connect-clients-outside-vpc.html)
- [Cloudera AI Inference authentication](https://docs.cloudera.com/machine-learning/cloud/ai-inference/topics/ml-caii-authentication.html) · [Making a call with the OpenAI API](https://docs.cloudera.com/machine-learning/cloud/ai-inference/topics/ml-caii-make-inference-call-model-endpoint-with-openai-api.html)
- [Apache Flink Agents](https://github.com/apache/flink-agents) · [Flink Agents deployment docs (0.3)](https://nightlies.apache.org/flink/flink-agents-docs-release-0.3/docs/operations/deployment/)
- [Official Flink images](https://hub.docker.com/_/flink)
