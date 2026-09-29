# FlinkAgentsDeployment

Deploying [Apache Flink Agents](https://github.com/apache/flink-agents) on Cloudera, two ways.

| | [CSA Operator on AKS](./flink-agents-on-cdf-azure.md) | [CSA Data Hub on YARN](./flink-agents-on-csa-datahub.md) |
|---|---|---|
| Substrate | Kubernetes, `FlinkDeployment` CRD | YARN, `flink run` from the gateway |
| Flink version | **You pin it** (`flink:1.20.5-java17`) | Whatever the CSA parcel ships |
| Runtime image | Yours, built and pushed to ACR | Cloudera's parcel, not modifiable |
| Language | Java or Python | **PyFlink** |
| Access needed | Helm/kubectl on the AKS cluster | **SSH to the Data Hub gateway** |
| Cloudera support | Supported deployment model | **Unsupported** — PyFlink is not covered on CSA |

**Choose the operator path** when you control the cluster, want a pinned Flink version and image, or
need this in production. It is the recommended route.

**Choose the Data Hub path** when a Streaming Analytics Data Hub is the substrate you have been given,
you can SSH to its gateway, and you want to run an existing PyFlink Flink Agents job on it without
standing up your own operator. It works — verified end to end on CSA 1.18.0.0 / Flink 1.20.5 — but it
is unsupported and it depends on CSA implementation details that an upgrade can change.

If you are on the Data Hub path and short on time, read [the one finding that matters
most](./flink-agents-on-csa-datahub.md#the-one-finding-that-matters-most) first: **on CSA, `flink run
-D<key>=<value>` is silently discarded for every key — use `-yD`.** It is the single biggest time sink
on that route.

Supporting artifacts for the Data Hub path — probe, build, submit, and the runtime patch — are in
[`datahub/`](./datahub/).
