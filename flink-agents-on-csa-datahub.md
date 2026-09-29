# Running Flink Agents on a CSA Data Hub (PyFlink on YARN)

This is the **Data Hub / YARN** path: take a Flink Agents job that already works on a local Docker
Flink cluster and run it, unchanged in shape, as a YARN job on a Cloudera **Streaming Analytics**
Data Hub in CDP Public Cloud. No operator, no custom image, no Kubernetes.

The sibling document [`flink-agents-on-cdf-azure.md`](./flink-agents-on-cdf-azure.md) covers the other
path — the CSA Operator on AKS, where you build and pin your own image. Read
[README.md](./README.md) for when to choose which.

## Status: this works, and it is unsupported

Verified end to end on 2026-09-29:

| | |
|---|---|
| Platform | CSA 1.18.0.0, **Apache Flink 1.20.5**, Runtime 7.3.2, Java 17, Scala 2.12 |
| Cluster | Streaming Analytics Light Duty Data Hub — 1 gateway, 2 masters, 3 workers, Kerberos via FreeIPA, Ranger **RAZ** enabled, S3 storage |
| Job | `workflow_counter` — a PyFlink DataStream job wrapping a Flink Agents `CounterAgent` |
| Result | YARN application **FINISHED / SUCCEEDED**, one AM attempt, 4 tasks `RUNNING → FINISHED` |

Emitting exactly the expected records, once each:

```
{'input': 5,  'doubled': 10, 'agent': 'workflow_counter'}
{'input': 10, 'doubled': 20, 'agent': 'workflow_counter'}
{'input': 15, 'doubled': 30, 'agent': 'workflow_counter'}
```

**Cloudera does not support PyFlink on CSA.** The CSA unsupported-features list calls out "Virtual
environments for Python," and no Cloudera documentation describes running a PyFlink DataStream job on
a CSA Data Hub. Flink Agents additionally needs **pemja**, a JNI extension that embeds libpython
inside the TaskManager JVM. Everything below is a working configuration, not a supported one. Do not
put it under a production SLA, and expect a CSA upgrade to be able to break it — see
[What is not supported](#what-is-not-supported).

---

## The one finding that matters most

> ### On CSA, `flink run -D<key>=<value>` is silently discarded. For every key. Use `-yD`.

No warning, no "unknown option", no rejected value. The flags simply never reach the cluster, so you
end up debugging the consequences of defaults you are convinced you overrode. This cost the bulk of
the time spent on this exercise; if you read nothing else here, read this.

**Why**, read out of the parcel's own classes rather than inferred:

1. Cloudera Manager writes `execution.target: yarn-per-job` into `/etc/flink/conf/flink-conf.yaml`.
2. `CliFrontend` selects the first `CustomCommandLine` whose `isActive()` returns true.
   `FlinkYarnSessionCli.isActive()` tests
   `YarnJobClusterExecutor.NAME.equals(configuration.get(DeploymentOptions.TARGET))` against the
   **file** configuration — against that exact key. So it is active on *every* submission on CSA.
3. It is registered **before** `GenericCLI`, so it wins. You can see this in `flink run --help`,
   which prints each CLI's section in registration order: "Options for yarn-cluster mode" appears
   before "Options for Generic CLI mode".
4. `FlinkYarnSessionCli` reads dynamic properties from its own `-yD` option and has **no `-D`
   handling at all**. `GenericCLI`'s `DynamicPropertiesUtil.encodeDynamicProperties()` — the only
   code that consumes `-D` — never runs.
5. `-D` is still a *defined* commons-cli option, so it parses without complaint. `flink run -D` with
   no argument fails with `Missing argument for option: D`, which proves the option exists and its
   value is simply never read.

Proven both directions on a plain Java job (`examples/streaming/WordCount.jar` — no Python, no agents
jars, so it is not a PyFlink argument-ordering quirk):

| flag | result |
|---|---|
| `-D taskmanager.numberOfTaskSlots=3` | JobManager loaded `1`, TaskManager registered `numSlots=1` |
| `-yD taskmanager.numberOfTaskSlots=3` | JobManager loaded `3`, TaskManager registered `numSlots=3` |

**Three dead ends already ruled out — do not re-walk them:**

- **Not** the Cloudera Manager wrapper. `/usr/bin/flink` → alternatives → the parcel's `bin/flink`;
  `flink-exec-env.sh` only exports environment variables; the parcel's `bin/flink` ends in a stock
  `exec … CliFrontend "$@"`.
- **Not** `flink-conf.yaml` overriding the CLI. Two probe keys absent from that file entirely still
  had their `-D` values vanish.
- **Not** observable via `yarn.application.name`. `YarnClusterDescriptor.deployJobCluster()`
  **hardcodes** the literal string `"Flink per-job cluster"` in per-job mode, so that key is ignored
  however you pass it. Consequence: **never grep for your job name in `yarn application -list`.**

`-yD` applies to `security.kerberos.login.keytab` / `.principal` too. A keytab passed as `-D` is
ignored and the job silently falls back to the submitting user's ticket cache — which expires and
kills a long-running job hours later for no visible reason.

The **exception** is the dedicated PyFlink dependency flags — `-pyfs`, `-pyarch`, `-pyexec`,
`-pyclientexec`, `-pypath`. Those are parsed by `CliFrontend` / `PythonDependencyUtils` from their
own options, not from dynamic properties, so they work as documented. Keep them as dedicated flags
rather than converting them to `-yD`: that also keeps the command portable to clusters where `-D`
works normally.

---

## Prerequisites

- A Streaming Analytics Data Hub you can **SSH into**. You need the gateway node
  (`*-manager0`, role `GATEWAY_PRIMARY`). If SSH to the nodes is not available to you, this path is
  closed — use the operator path instead.
- The SSH key registered with the CDP environment, loaded into your agent:
  `ssh-add --apple-use-keychain ~/.ssh/<your-key>` on macOS. Use `ssh -A` through the gateway if you
  need to reach worker nodes.
- A **CDP workload** username and password (set in the CDP Console under your user profile), and a
  Kerberos ticket on the gateway: `kinit <workload-user>`. Your CDP *console* login is not the same
  identity and cannot reach HDFS or YARN.
- `git`, `mvn` and Docker locally, only to build the `flink-agents` jars and wheel once.
- The source project providing the agents — this runbook was built against
  [`BrooksIan/FlinkDockerWithAgents`](https://github.com/BrooksIan/FlinkDockerWithAgents) ("ratatoskr").
  The artifacts in [`datahub/`](./datahub/) are what that project needs *added* to run on CSA; they do
  not replace it.

---

## Step 0. Probe the cluster before building anything

These are cheap and they can kill the approach in ten minutes instead of after a day of building.
On the gateway:

```bash
# Resolve FLINK_HOME — the parcel name varies by CSA version
ls -d /opt/cloudera/parcels/FLINK*            # e.g. FLINK-1.20.5-csa1.18.0.0-82229597
export FLINK_HOME=/opt/cloudera/parcels/FLINK/lib/flink
export FLINK_CONF_DIR=/etc/flink/conf

ls $FLINK_HOME/lib/flink-dist*.jar            # the authoritative Flink version
ls $FLINK_HOME/lib/flink-python*.jar          # DECISIVE — is PyFlink in the parcel?
python3 --version; ls -d /usr/local/lib64/python3.*/site-packages
ls /usr/lib64/libpython3.*.so*                # pemja needs a libpython to embed
cat /etc/os-release                            # OS family and version
kinit <workload-user> && klist                 # Kerberos works
yarn node -list                                # YARN reachable, capacity free
hdfs dfs -ls /user/<workload-user>             # HDFS home exists (create if not)
```

`flink-python-*.jar` missing from `$FLINK_HOME/lib/` is the single no-go: PyFlink has been stripped
from the parcel and no amount of client-side work will recover it. Go to the operator path.

[`datahub/scripts/probe_csa_gateway.sh`](./datahub/scripts/probe_csa_gateway.sh) runs all of this and
prints a summary.

### What Step 0 found, and why it changed everything

**CSA nodes already ship a complete, matched, pip-installed PyFlink stack.** Verified on the gateway
*and* independently on all three workers:

```
/usr/local/lib64/python3.11/site-packages     (installed by Cloudera, not rpm-owned)
  apache-flink 1.20.5     apache-flink-libraries 1.20.5   apache-beam 2.48.0
  pemja 0.5.7             numpy 1.24.4                    pyarrow 11.0.0
  pandas 2.2.3            protobuf 4.23.4                 py4j 0.10.9.7
/usr/lib64/libpython3.11.so.1.0                on every node
$FLINK_HOME/lib/flink-python-1.20.5-csa1.18.0.0.jar   already on the system classpath
```

This replaced a 626MB cross-compiled conda-pack archive with a **13MB venv built on the gateway**,
and it removed the single largest source of risk in the whole exercise:

> **pemja stops being a guess.** pemja is a JNI bridge — the Java classes in `flink-python-*.jar` and
> the Python `pemja_core*.so` **must be the same version**. Cloudera **patches this pin**: their
> `apache-flink 1.20.5` requires `pemja>=0.5.7,<0.5.8`, where upstream `apache-flink 1.20.1` pins
> `pemja==0.4.1`. A bundle resolved from PyPI therefore contains pemja 0.4.1. It installs cleanly,
> imports cleanly, passes a clean-container check — and dies inside the TaskManager JVM, where the
> only way to test it is to submit a job.

Build against what the node has. Do not resolve this stack from PyPI.

---

## Step 1. Build the jars and the wheel (once, locally)

`flink-agents` publishes a per-Flink-minor `dist/` module, and `release-0.3` **does** include
`dist/flink-1.20` — which is what makes this feasible at all. You need three artifacts:

- `flink-agents-dist-common-0.3-SNAPSHOT.jar`
- `flink-agents-dist-flink-1.20-0.3-SNAPSHOT-thin.jar`
- `flink_agents-*.whl`

[`datahub/deploy/Dockerfile.csa-build`](./datahub/deploy/Dockerfile.csa-build) builds them in a
`linux/amd64` container (your Mac is arm64; the wheel's native pieces must match the cluster).
[`datahub/scripts/build_csa_bundle.sh`](./datahub/scripts/build_csa_bundle.sh) drives it and assembles
`dist/csa/`.

Two things in that Dockerfile are load-bearing and easy to lose: `SKIP_SPOTLESS_CHECK=true`, and a
`PIP_CONSTRAINT` of `setuptools>=75.3,<82`.

> The Docker image is large. If you hit `no space left on device` mid-build, that is the image layer
> cache, not your repo — `docker system prune -a` and retry. You only need this image once; after the
> jars and wheel exist you can delete it.

`SKIP_DOCKER=1 scripts/build_csa_bundle.sh` reassembles only the agent-code zip, which is what you
want on every iteration after the first.

## Step 2. Build the Python environment on the gateway

```bash
scp dist/csa/wheel/flink_agents-*.whl datahub/scripts/build_csa_venv_gateway.sh \
    <user>@<gateway>:~/ratatoskr-csa/
ssh <user>@<gateway>
cd ~/ratatoskr-csa && ./build_csa_venv_gateway.sh
```

This creates a venv with `--system-site-packages` so it *inherits* the node's matched PyFlink and
pemja rather than reinstalling them, and adds only `flink_agents` and its genuinely-missing
dependencies. Built on the target OS, for the target arch — no cross-compilation, no relocation.

## Step 3. Ship the bundle and submit

```bash
scp -r dist/csa/ <user>@<gateway>:~/ratatoskr-csa/
ssh <user>@<gateway>
kinit <workload-user>
cd ~/ratatoskr-csa
DRY_RUN=1 ./submit_agent_csa.sh     # print the assembled command, submit nothing
./submit_agent_csa.sh               # submit
```

[`datahub/scripts/submit_agent_csa.sh`](./datahub/scripts/submit_agent_csa.sh) is the whole submit
path, and it re-checks on the gateway the things that can only be checked there: that the parcel ships
PyFlink, that the bundle's Flink minor matches the cluster's, and that the venv can import
`pemja` / `pyflink` / `flink_agents`. Always run `DRY_RUN=1` first.

The command it assembles, reduced to its essentials:

```bash
$FLINK_HOME/bin/flink run \
  -t yarn-per-job \
  -d \
  -yD yarn.application-attempts=2 \
  -yD yarn.application-attempt-failures-validity-interval=-1 \
  -yD classloader.parent-first-patterns.additional='pemja;software.amazon.awssdk' \
  -yD yarn.classpath.include-user-jar=LAST \
  -yD execution.checkpointing.checkpoints-after-tasks-finish.enabled=false \
  -yD security.kerberos.login.keytab=<keytab> \
  -yD security.kerberos.login.principal=<workload-principal> \
  -pyexec       /usr/bin/python3.11 \
  -pyclientexec $PWD/agentenv/bin/python \
  -pyfs         $PWD/agentcode.zip \
  -py           $PWD/run_workflow_cluster_csa.py
```

Every line of that is there for a reason found the hard way. The next section explains the two that
are not obvious.

---

## The JobManager crash loop with no visible cause

**Symptom.** The YARN application starts, the ApplicationMaster dies after ~22 seconds, YARN restarts
it, and it dies again — 50+ times. Nothing in the loop mentions Python, agents, or any jar you
supplied.

**Cause.** `flink-agents-dist-common` bundles an entire **AWS SDK v2** (~4128 classes, presumably for
Bedrock). On a **RAZ**-enabled cluster it shadows the platform's SDK, and Ranger's S3 signer throws
while committing checkpoint metadata to `s3a://`:

```
java.lang.NoSuchMethodError: Checksummer.forFlexibleChecksum(
    String, ChecksumAlgorithm, PayloadChecksumStore)
  at org.apache.ranger.raz.hook.s3.RazS3SignerPlugin.addCheckSumToRequest(...:163)
```

Attributed by the stack trace's own jar annotations, not guessed: every `software.amazon.awssdk.*`
frame resolved from `~[flink-agents-dist-common-0.3-SNAPSHOT.jar]`, while `RazS3SignerPlugin`
resolved from `~[ranger-raz-hook-s3-2.8.0.7.3.2.20000-258.jar]`. RAZ is compiled against the
platform's newer SDK; the bundled copy lacks the `PayloadChecksumStore` overload.

It presents as a total mystery because `FatalExitExceptionHandler` treats any uncaught exception on
`jobmanager-io-thread-N` as fatal and **halts the JVM**. The AM exits 239 and YARN restarts it.

**Fix — `-yD yarn.classpath.include-user-jar=LAST`.** In yarn-per-job mode Flink copies
`pipeline.jars` onto the JobManager's **system** classpath, positioned by that key, whose default
`ORDER` means "sorted in among Flink's own jars" — and `flink-agents` sorts before `flink-s3`.
Measured from the JobManager's own logged `Classpath:` line:

| jar | default `ORDER` | with `LAST` |
|---|---|---|
| `flink-agents-dist-common-0.3-SNAPSHOT.jar` | **5** | **53** |
| `lib/flink-s3-fs-hadoop-1.20.5-csa1.18.0.0.jar` | 32 | 29 |
| `lib/ranger-raz-hook-s3-…jar` | 45 | 42 |

`LAST` rather than `DISABLED` deliberately — the common jar must stay visible to the JobManager's
system classloader for the JM-side `CompileUtils` path.

**Two traps around this:**

- **The key name.** `yarn.per-job-cluster.include-user-jar` is a **deprecated alias that is silently
  ignored** — it left the JM classpath byte-for-byte unchanged. The live key is
  `yarn.classpath.include-user-jar`.
- **`classloader.parent-first-patterns.additional` cannot fix this alone.** It governs only the
  *user-code* classloader, and nothing in that stack trace (`S3AFileSystem`, `RazS3SignerPlugin`) is
  user code. Keep `pemja` there regardless — that one is needed for
  [FLINK-39226](https://issues.apache.org/jira/browse/FLINK-39226) — but classpath *order* is what
  resolves this.

You cannot dodge it by suppressing checkpoints, either: `jobmanager.archive.fs.dir` is on `s3a://`
too, so job *archiving* hits the identical error at termination. Repair the classpath.

### Why the attempt cap does not stop the loop

`yarn.resourcemanager.am.max-attempts=2` looks like it should bound this. It does not.
`yarn.application-attempt-failures-validity-interval` defaults to **10000 ms**, and YARN *forgets* a
failure older than that window — so any crash loop slower than 10s per cycle never accumulates toward
the cap. Set the interval to `-1` so the cap actually caps. (CSA's `flink-conf.yaml` also sets
`yarn.application-attempts: 5`.)

**Honest gap:** three flags were applied in the same submission that first succeeded
(`include-user-jar=LAST`, the parent-first patterns, and `checkpoints-after-tasks-finish.enabled=false`),
so which of them is *individually* necessary is untested. Also unidentified: what triggers the
`JobMaster - Triggering a manual checkpoint` seen ~400 ms after `RUNNING` — ruled out are the agents
jars, `flink_agents`' own `execute()`, and an external REST call.

---

## The rest of the traps, each verified

**`-pyfs` zip layout: packages must be the zip's own top-level entries.**
`AbstractPythonEnvironmentManager.constructFilesDirectory()` expands the zip *into* the very directory
it then puts on the worker's `PYTHONPATH`. So `agentcode.zip` must contain `ratatoskr/` and
`examples/` at its root. Wrap them in any directory and they are invisible, with a
`ModuleNotFoundError` that looks like a packaging bug in your own code.

**`python.archives` contributes nothing to `PYTHONPATH`.** Only `python.files` (`-pyfs`) and
`python.pythonpath` (`-pypath`) do. An archive shipped with `-pyarch` is extracted and is useful for
an *interpreter* you then point `-pyexec` at; it does not make its contents importable. This is
easy to misread from the Flink docs.

**PyFlink's `add_jars` appends to `pipeline.jars`.** It does not replace it. Passing the agents jars
*both* via `-yD pipeline.jars` and via the code path duplicates them, which breaks the single-classloader
invariant pemja depends on. Attach them in exactly one place — this project does it in code, in a
single `add_jars(*jar_uris)` call.

**PyFlink 1.20 has no `disable_checkpointing()`.** It exists in 2.x. On 1.20 you configure it, and on
CSA that means `-yD`.

**Flink has two config formats, and CSA ships the legacy one.** The newer `config.yaml` is real YAML;
the legacy flat `flink-conf.yaml` is parsed by a hand-rolled line splitter in `GlobalConfiguration`
and is **not required to be valid YAML**. CSA's contains
`yarn.container-start-command-template`, whose value has Flink's own `%java% %jvmmem%` placeholders —
and a bare `%` cannot start a YAML token. Anything that hands that file to `yaml.safe_load` crashes:

```
yaml.scanner.ScannerError: found character '%' that cannot start any token
  in "/etc/flink/conf/flink-conf.yaml", line 49, column 40
```

`flink_agents`' `RemoteExecutionEnvironment.__load_config_from_flink_conf_dir` does exactly that. The
patch in [`datahub/patches/`](./datahub/patches/) parses both formats and is a no-op wherever the file
is valid YAML. Note this is reachable *only because* `FLINK_CONF_DIR` is set — and unsetting it is not
an option, since the parcel's scripts, `--add-opens` flags, and every YARN/HA/security default depend
on it. Also note the parse is pointless either way: the loader keeps only `agent.*` keys, and CSA's
config has none.

**Do not `sed` the parcel's config.** A `config.yaml` you expect to be there is not; the file is
`flink-conf.yaml`, it lives in `/etc/flink/conf`, and it is Cloudera-Manager-generated. Editing
cluster config belongs in CM, not in a submit script.

**A `noexec` `/tmp` breaks pemja.** The JNI extension is extracted and `dlopen`'d at runtime; if
`/tmp` is mounted `noexec` this fails with a `dlopen` / permission error that names nothing useful.
Point the temp directory somewhere executable.

**Getting YARN logs from a crash loop.** While an application is `RUNNING`, log aggregation retains
only the **live** container, so `yarn logs -applicationId <id>` shows you the current attempt and
hides the 50 before it. Kill the app first, *then* aggregate:

```bash
yarn application -kill <appId>
yarn logs -applicationId <appId> > app.log     # now contains every attempt
```

And since `yarn.application.name` is ignored in per-job mode, find your application by user and start
time, or grep for the hardcoded literal:

```bash
yarn application -list | grep 'Flink per-job cluster'
```

---

## What is not supported

State this plainly to anyone who asks to run it in production:

- **PyFlink on CSA is outside Cloudera support.** "Virtual environments for Python" is on the CSA
  unsupported-features list, and no Cloudera documentation covers PyFlink DataStream jobs on a Data Hub.
- **`flink-agents` `release-0.3` is a `0.3-SNAPSHOT` build from source.** There is no released
  artifact, no compatibility guarantee, and the jar names carry `-SNAPSHOT`.
- **This configuration depends on CSA implementation details.** The `-yD`-not-`-D` behaviour follows
  from Cloudera Manager presetting `execution.target`; the classpath fix depends on the relative sort
  order of jar names; the pemja version match depends on Cloudera's patched pin. A CSA upgrade can
  change any of the three. Re-run Step 0 after every upgrade.
- **`-t yarn-per-job` is deprecated in Flink 1.20.** It still works, and it is the right choice here
  because local `-py` / `-pyfs` paths work without staging everything to HDFS first. Moving to
  `run-application -t yarn-application` is the follow-up.
- **This was validated on one job with no external I/O.** `workflow_counter` has a bounded source, no
  Kafka, and no LLM. Kafka (the cluster's own SASL_SSL broker on 9093) and ReAct agents against
  Cloudera AI Inference are additive next steps, not things this runbook has proven.

## Security notes

- Use a **keytab** via `-yD security.kerberos.login.keytab` / `.principal` for anything long-running.
  The `kinit` ticket cache is fine for a short test and will silently kill a long job when it expires.
- **`/etc/flink/conf/flink-conf.yaml` on a CM-managed cluster contains a plaintext truststore
  password.** Keep that file, and any dump of the JobManager's JVM options, out of runbooks, pastes,
  screenshots and commits. If you must show JVM options, redact with an **allow-list** (print only
  the keys you intend to show) — a deny-list `sed` will mask the paths and leak the values.
- The gateway's `cloudbreak` user has passwordless `sudo`. Nothing in this runbook needs it. None of
  this work modified, installed, or wrote anything on any cluster node — every cluster file was read
  only.

## What is in `datahub/`

| Path | What it is |
|---|---|
| [`scripts/probe_csa_gateway.sh`](./datahub/scripts/probe_csa_gateway.sh) | Step 0's go/no-go probes, as one script |
| [`deploy/Dockerfile.csa-build`](./datahub/deploy/Dockerfile.csa-build) | `linux/amd64` build of the `flink-agents` 1.20 jars and wheel |
| [`scripts/build_csa_bundle.sh`](./datahub/scripts/build_csa_bundle.sh) | Drives that build, assembles `dist/csa/` |
| [`scripts/build_csa_venv_gateway.sh`](./datahub/scripts/build_csa_venv_gateway.sh) | Builds the venv **on the gateway**, against the node's own PyFlink |
| [`scripts/submit_agent_csa.sh`](./datahub/scripts/submit_agent_csa.sh) | The submit path. Supports `DRY_RUN=1`. Heavily commented with the findings above |
| [`examples/run_workflow_cluster_csa.py`](./datahub/examples/run_workflow_cluster_csa.py) | The job entry point |
| [`patches/ratatoskr-runtime-csa-portability.patch`](./datahub/patches/ratatoskr-runtime-csa-portability.patch) | Makes the ratatoskr runtime `FLINK_HOME`-driven and adds the legacy-config fallback. Applies to `BrooksIan/FlinkDockerWithAgents`; the local Docker path is unchanged |

The scripts are parameterized — no hostnames, usernames, or environment names baked in. Cluster names
appear only in `Verified on …` provenance comments.

## References

- [Apache Flink Agents](https://github.com/apache/flink-agents) · [deployment docs (0.3)](https://nightlies.apache.org/flink/flink-agents-docs-release-0.3/docs/operations/deployment/)
- [FLINK-39226 — pemja `ClassCastException` across classloaders](https://issues.apache.org/jira/browse/FLINK-39226)
- [Flink 1.20 — YARN deployment](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/yarn/) · [Python dependency management](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/dependency_management/)
- [Cloudera Streaming Analytics — unsupported features](https://docs.cloudera.com/csa/latest/release-notes/topics/csa-unsupported-features.html)
- Cloudera's own secure-YARN Flink recipe, which uses `-yD` throughout:
  [`flink-secure-tutorial`](https://github.com/cloudera/flink-tutorials/tree/master/flink-secure-tutorial)
