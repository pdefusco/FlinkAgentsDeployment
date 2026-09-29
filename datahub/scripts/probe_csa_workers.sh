#!/usr/bin/env bash
# Phase 0b: prove the PyFlink runtime exists ON THE WORKER NODES, inside a real YARN
# container — not on the gateway, where it proves nothing about where the job runs.
#
#   scp scripts/probe_csa_workers.sh <user>@<gateway>:~/
#   ssh <user>@<gateway>
#   kinit <workload-user>
#   DRY_RUN=1 bash ~/probe_csa_workers.sh        # show the plan, submit nothing
#   bash ~/probe_csa_workers.sh | tee csa-workers.txt
#
# *** THIS ONE IS NOT READ-ONLY. *** probe_csa_gateway.sh changes nothing; this script
# SUBMITS A REAL YARN APPLICATION (a distributed-shell app, one container per worker,
# ~30-60s, self-terminating, shows up as a normal FINISHED/SUCCEEDED app in the RM UI and
# in aggregated logs). It writes nothing on any node outside the containers' own working
# directories and YARN's staging dir under your HDFS home. It is cheap and harmless, but
# it is a submission to a shared cluster — confirm with the cluster owner first, and run
# DRY_RUN=1 beforehand so you can see exactly what will be requested.
#
# WHY THIS EXISTS — the gateway is not the cluster
# -----------------------------------------------
# The whole `--system-site-packages` design (see build_csa_venv_gateway.sh) rests on one
# claim: every node already has Cloudera's matched PyFlink stack in
# /usr/local/lib64/python3.11/site-packages, plus /usr/lib64/libpython3.11.so.1.0 for
# pemja to embed. On the strength of that claim submit_agent_csa.sh stopped shipping an
# interpreter archive via -pyarch at all. If the claim is false on even one worker, jobs
# fail *intermittently* — they succeed whenever the TaskManagers happen to avoid that node
# — which is the worst possible failure mode to debug.
#
# And you cannot check it the obvious way. On a CDP Data Hub you can ssh to the gateway as
# the cloud OS user, but **ssh from the gateway to the workers is refused**. The gateway's
# own site-packages tells you nothing either, because that directory is populated by a
# Cloudera service recipe rather than owned by an rpm — so it is not guaranteed to be
# identical across instance groups, and `rpm -q` cannot answer the question at all.
#
# A YARN distributed-shell app is the way in. It runs your script inside a real container,
# as the same user, under the same environment a TaskManager gets — so it proves the
# library is *importable there*, not merely present on some disk somewhere.
#
# THE FOUR THINGS THAT COST REAL ITERATIONS (all encoded below — don't undo them)
# ------------------------------------------------------------------------------
#  1. CONTAINER SIZE DECIDES COVERAGE. With small containers YARN happily packs every one
#     of them onto a single node. Three separate runs at 256MB, 1024MB and 4915MB all
#     landed entirely on worker1 — verifying one node three times while looking like full
#     coverage. The fix is arithmetic, not luck: make each container LARGER THAN HALF a
#     node's memory capacity and two cannot co-locate. Hence CONTAINER_MEMORY is derived
#     from the measured capacity (MEM_FRACTION, default 0.6) rather than defaulted.
#  2. PARSE THE CAPACITY BY LABEL. `yarn node -status` prints `Memory-Used : 0MB` BEFORE
#     `Memory-Capacity : 21504MB`, so `grep -oE '[0-9]+MB' | head -1` silently returns 0
#     and every derived size is nonsense. That wrong parse is what caused (1). Match the
#     label.
#  3. PREFIX EVERY LINE WITH THE HOSTNAME, INSIDE THE CONTAINER. Otherwise identical
#     results from different nodes are indistinguishable, `sort -u` collapses them into one
#     block, and one node's answer reads as the whole cluster's. Every line the probe emits
#     goes through pfx() and carries PROBE[<host>].
#  4. USE -shell_script, NOT -shell_command. Quoting a multi-line Python heredoc through
#     -shell_command is unusable. -shell_script ships a local file to every container.
#
# Deliberately NOT `set -e` in the body, for the same reason as probe_csa_gateway.sh: each
# check is independent and the point is one complete picture per round-trip. It DOES exit
# non-zero at the end if a node reported INCOMPLETE or if coverage was partial — unlike the
# gateway probe, this script has a real machine-checkable verdict, so it is worth returning.
#
# PROVENANCE — be precise about what has and has not been run
# ----------------------------------------------------------
# The FINDINGS are from a live cluster. On 2026-09-29, on pdf-pwc (CSA 1.18.0.0 / Flink
# 1.20.5, RHEL 9.6, CDH 3.4.2.7.3.2.20000-258), an ad-hoc version of this probe ran under
# distributed-shell across all three workers and each reported: libpython3.11.so.1.0
# present, pemja 0.5.7, apache-flink 1.20.5, apache-beam 2.48.0, numpy 1.24.4, pyarrow
# 11.0.0, pandas 2.2.3, and `import pyflink+pemja: OK`. The container-sizing and
# sort -u/hostname traps above were all hit for real on the way there.
#
# THIS SCRIPT is the formalization of that, and has been exercised only against stubbed
# `yarn node -list` / `-status` / `logs` output — the parse, the sizing arithmetic, the
# clamps, the coverage count and all three exit codes. It has not itself been run end to end
# on a live cluster. So: run DRY_RUN=1 first and read the numbers, rather than assuming this
# file is proven the way submit_agent_csa.sh is.

section() { printf '\n========== %s ==========\n' "$*"; }
note()    { printf '    %s\n' "$*"; }
fail()    { printf '\nERROR: %s\n' "$*" >&2; exit 1; }

# --- Tunables. Everything is an environment variable; no positional arguments. ----------
# SYS_PY must be the interpreter whose site-packages holds the node's PyFlink — the same
# value build_csa_venv_gateway.sh and submit_agent_csa.sh use, or this proves the wrong
# thing. PKGS is the list this design actually depends on; add to it, don't trim it.
SYS_PY="${SYS_PY:-/usr/bin/python3.11}"
PKGS="${PKGS:-apache-flink apache-flink-libraries apache-beam pemja numpy pyarrow pandas}"
APP_NAME="${APP_NAME:-csa_worker_probe}"
NUM_CONTAINERS="${NUM_CONTAINERS:-}"        # default: one per RUNNING node
CONTAINER_MEMORY="${CONTAINER_MEMORY:-}"    # default: derived from measured capacity
MEM_FRACTION="${MEM_FRACTION:-0.6}"         # must be > 0.5 or containers can co-locate
MASTER_MEMORY="${MASTER_MEMORY:-512}"
PROBE_TIMEOUT_MS="${PROBE_TIMEOUT_MS:-300000}"
LOG_RETRIES="${LOG_RETRIES:-6}"             # log aggregation lags app completion
LOG_RETRY_SLEEP="${LOG_RETRY_SLEEP:-10}"
DS_JAR="${DS_JAR:-}"                        # default: auto-detected
DRY_RUN="${DRY_RUN:-0}"
KEEP_TMP="${KEEP_TMP:-0}"

printf 'CSA worker probe — %s from %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(hostname -f 2>/dev/null || hostname)"

# --- Preflight: this script can only run from a gateway with a YARN client and a ticket --
section "0. Preflight"
command -v yarn >/dev/null 2>&1 || fail "yarn is not on PATH. Run this on the CSA gateway node."
note "yarn: $(command -v yarn)"

# A submission needs Kerberos. Checked rather than attempted, because the failure from a
# missing ticket surfaces as an opaque GSS/SIMPLE-auth error several seconds into the
# submit. Note the identity that matters is your KERBEROS principal, not the OS user you
# ssh'd in as — YARN and HDFS stage as the principal.
if klist -s 2>/dev/null; then
  note "kerberos: $(klist 2>/dev/null | sed -n 's/^Default principal: //p')"
else
  note "kerberos: NO VALID TICKET"
  [ "$DRY_RUN" = "1" ] || fail "No Kerberos ticket. Run: kinit <workload-user>
(DRY_RUN=1 works without one — it submits nothing.)"
fi

# The distributed-shell jar is part of the CDH parcel. Path varies by layout, so glob for
# it in the places it has actually been found rather than hardcoding one.
if [ -z "$DS_JAR" ]; then
  for g in \
    /opt/cloudera/parcels/CDH/lib/hadoop-yarn/hadoop-yarn-applications-distributedshell*.jar \
    /opt/cloudera/parcels/CDH/jars/hadoop-yarn-applications-distributedshell*.jar \
    /usr/lib/hadoop-yarn/hadoop-yarn-applications-distributedshell*.jar
  do
    [ -f "$g" ] && DS_JAR="$g" && break
  done
fi
[ -n "$DS_JAR" ] || fail "No hadoop-yarn-applications-distributedshell jar found.
Locate it and pass it explicitly:
  DS_JAR=\$(find /opt/cloudera/parcels -name 'hadoop-yarn-applications-distributedshell*.jar' | head -1) \\
  bash $0"
note "distributedshell: $(basename "$DS_JAR")"

# --- Measure the cluster, then size the containers from what was measured ---------------
section "1. Nodes and capacity (this is what decides coverage)"
NODE_LIST="$(yarn node -list 2>/dev/null)"
NODES=$(printf '%s\n' "$NODE_LIST" | awk '$2=="RUNNING" {print $1}')
NODE_COUNT=$(printf '%s\n' "$NODES" | grep -c ':' )
[ "$NODE_COUNT" -gt 0 ] || fail "yarn node -list reported no RUNNING nodes:
$NODE_LIST"
note "RUNNING NodeManagers: $NODE_COUNT"

# Per node: capacity, and what is already used. Capacity sets the anti-co-location floor;
# free memory decides whether the request can actually be satisfied right now. Both are
# parsed BY LABEL — see gotcha (2) in the header.
MIN_CAP=""; MIN_FREE=""
for n in $NODES; do
  ST="$(yarn node -status "$n" 2>/dev/null)"
  CAP=$(printf '%s\n' "$ST" | sed -n 's/.*Memory-Capacity[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -1)
  USED=$(printf '%s\n' "$ST" | sed -n 's/.*Memory-Used[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -1)
  [ -n "$CAP" ] || { note "$n  capacity UNPARSEABLE — skipping in the sizing calculation"; continue; }
  [ -n "$USED" ] || USED=0
  FREE=$(( CAP - USED ))
  note "$(printf '%-44s capacity %6sMB  used %6sMB  free %6sMB' "$n" "$CAP" "$USED" "$FREE")"
  { [ -z "$MIN_CAP" ]  || [ "$CAP"  -lt "$MIN_CAP" ];  } && MIN_CAP="$CAP"
  { [ -z "$MIN_FREE" ] || [ "$FREE" -lt "$MIN_FREE" ]; } && MIN_FREE="$FREE"
done
[ -n "$MIN_CAP" ] || fail "Could not parse Memory-Capacity from any node.
Check the output of: yarn node -status $(printf '%s' "$NODES" | head -1)"

# yarn.scheduler.maximum-allocation-mb is a hard ceiling on a single container. Exceeding
# it gets the request rejected outright (InvalidResourceRequestException), so respect it.
# Best-effort XML read: yarn-site.xml is name/value pairs and the value follows the name.
# `< missing-file` is reported by the SHELL, not by tr, so 2>/dev/null on the command does
# not silence it — guard the redirect instead.
YARN_SITE="${YARN_SITE:-/etc/hadoop/conf/yarn-site.xml}"
MAX_ALLOC=""
if [ -r "$YARN_SITE" ]; then
  MAX_ALLOC=$(tr -d ' \t\n' < "$YARN_SITE" \
    | grep -o '<name>yarn.scheduler.maximum-allocation-mb</name><value>[0-9]*' \
    | grep -o '[0-9]*$' | head -1)
fi
[ -n "$MAX_ALLOC" ] && note "yarn.scheduler.maximum-allocation-mb: ${MAX_ALLOC}MB"

[ -n "$NUM_CONTAINERS" ] || NUM_CONTAINERS="$NODE_COUNT"
if [ "$NUM_CONTAINERS" -gt "$NODE_COUNT" ]; then
  note "NOTE: NUM_CONTAINERS=$NUM_CONTAINERS exceeds the $NODE_COUNT RUNNING nodes."
  note "      Extra containers must co-locate, so they verify a node twice. Harmless, but"
  note "      one-per-node is what you want; leave NUM_CONTAINERS unset."
fi

if [ -z "$CONTAINER_MEMORY" ]; then
  # Integer arithmetic on a fraction expressed as basis points, so no bc/awk dependency.
  FRAC_BP=$(printf '%s' "$MEM_FRACTION" | awk '{printf "%d", $1*1000}')
  CONTAINER_MEMORY=$(( MIN_CAP * FRAC_BP / 1000 ))
  # The ApplicationMaster also occupies a container, on one of these same nodes. Leave room
  # for it or that node cannot host a probe container and one worker goes unverified.
  CEIL=$(( MIN_CAP - MASTER_MEMORY ))
  [ "$CONTAINER_MEMORY" -gt "$CEIL" ] && CONTAINER_MEMORY="$CEIL"
  [ -n "$MAX_ALLOC" ] && [ "$CONTAINER_MEMORY" -gt "$MAX_ALLOC" ] && CONTAINER_MEMORY="$MAX_ALLOC"
fi
HALF=$(( MIN_CAP / 2 ))
note "container_memory: ${CONTAINER_MEMORY}MB  (half a node is ${HALF}MB)"
if [ "$CONTAINER_MEMORY" -le "$HALF" ]; then
  note "*** WARNING: not larger than half a node. YARN MAY PACK EVERY CONTAINER ONTO ONE"
  note "    NODE, and partial coverage looks identical to success. Raise MEM_FRACTION, or"
  note "    trust the per-node table in the SUMMARY rather than the verdict."
fi
if [ -n "$MIN_FREE" ] && [ "$CONTAINER_MEMORY" -gt "$MIN_FREE" ]; then
  note "NOTE: the busiest node has only ${MIN_FREE}MB free, less than one container. The app"
  note "      will WAIT for room rather than fail. If it hangs, either wait for the cluster"
  note "      to drain or accept partial coverage with a smaller MEM_FRACTION."
fi

# --- The probe that runs inside each container ------------------------------------------
# Generated rather than shipped as a second file so this stays a single scp, and so the
# package list and interpreter are parameterized in one place.
# X's LAST in the template. GNU mktemp accepts a trailing suffix (`.XXXXXX.sh`) but BSD
# mktemp does not — it creates a file called literally `...XXXXXX.sh`, which is not a temp
# file at all. The gateway is Linux, but someone will inevitably DRY_RUN this on a Mac.
PROBE="$(mktemp "${TMPDIR:-/tmp}/csa-worker-probe.XXXXXX")"
# PYV is derived from SYS_PY (python3.11 -> 3.11) so the libpython check tracks the
# interpreter instead of hardcoding a version that a future CSA release will change.
PYV="$(basename "$SYS_PY" | sed -n 's/^python//p')"
{
  cat <<EOF
SYS_PY="$SYS_PY"
PYV="$PYV"
PKGS="$PKGS"
EOF
  cat <<'PROBE_BODY'
# Runs INSIDE a YARN container on a worker node. Must be tolerant of everything: its whole
# job is to report, never to exit early.
Hn="$(hostname -s 2>/dev/null || hostname)"
# Gotcha (3): without this prefix, identical answers from different nodes are
# indistinguishable and `sort -u` makes one node look like the whole cluster.
pfx() { while IFS= read -r l; do printf 'PROBE[%s] %s\n' "$Hn" "$l"; done; }

{
  echo "container user   $(id -un 2>/dev/null || echo '?')"

  # pemja is a JNI extension that EMBEDS libpython into the TaskManager JVM, so the shared
  # library must exist on the node — a python binary alone is not enough. ldconfig first
  # because it is path-independent; the explicit globs cover a node whose cache is stale.
  if ldconfig -p 2>/dev/null | grep -q "libpython${PYV}\.so"; then
    echo "libpython${PYV}     present (ldconfig)"
  elif ls /usr/lib64/libpython${PYV}.so* /usr/lib/libpython${PYV}.so* >/dev/null 2>&1; then
    echo "libpython${PYV}     present (/usr/lib64)"
  else
    echo "libpython${PYV}     *** MISSING *** pemja cannot embed an interpreter on this node"
  fi

  # Does the Flink parcel reach the workers, and does it carry PyFlink here too? The
  # gateway having it does not guarantee the parcel was distributed and activated
  # everywhere.
  PJ="$(ls /opt/cloudera/parcels/*/lib/flink/lib/flink-python*.jar 2>/dev/null | head -1)"
  if [ -n "$PJ" ]; then echo "parcel flink-python  $(basename "$PJ")"
  else echo "parcel flink-python  *** NOT FOUND on this node ***"; fi

  if [ -x "$SYS_PY" ]; then
    echo "$SYS_PY  $("$SYS_PY" --version 2>&1)"
    PKGS="$PKGS" "$SYS_PY" - <<'PY' 2>&1
import os, sys
import importlib.metadata as md

bad = []
for p in os.environ.get("PKGS", "").split():
    try:
        print("%-24s %s" % (p, md.version(p)))
    except Exception:
        print("%-24s *** MISSING ***" % p)
        bad.append(p)

# Import `pemja`, never `pemja_core`. pemja_core is the JNI half and importing it from a
# plain CPython process ALWAYS fails with `undefined symbol: JNI_GetCreatedJavaVMs`,
# because the JVM supplies that symbol. That failure is correct behaviour, not a broken
# node — and it means nothing outside a running TaskManager can prove the JNI layer loads.
# What this check does prove is that the Python halves are installed and importable under
# the container's own environment, which is the part that was previously unknown.
try:
    import pyflink, pemja
    print("%-24s OK" % "import pyflink+pemja")
    # Must resolve to the NODE's install. If it resolves anywhere else, something has
    # shadowed Cloudera's matched stack and the pemja version pin is no longer guaranteed.
    print("%-24s %s" % ("pyflink from", pyflink.__file__.split("/site-packages/")[0]))
except Exception as e:
    print("%-24s %s: %s" % ("import FAILED", type(e).__name__, e))
    bad.append("import")

print("VERDICT: %s" % ("READY" if not bad else "INCOMPLETE (%s)" % ",".join(bad)))
PY
  else
    echo "$SYS_PY  *** ABSENT ***  available: $(ls /usr/bin/python3.* 2>/dev/null | tr '\n' ' ')"
    echo "VERDICT: INCOMPLETE (no $SYS_PY)"
  fi
} | pfx
PROBE_BODY
} > "$PROBE"
[ "$KEEP_TMP" = "1" ] || trap 'rm -f "$PROBE"' EXIT

# --- Assemble the submission -------------------------------------------------------------
section "2. The submission"
# -shell_script, not -shell_command: gotcha (4). The AM jar and the application jar are the
# same file, which is how the distributed-shell example is meant to be invoked.
set -- yarn jar "$DS_JAR" -jar "$DS_JAR" \
  -shell_script "$PROBE" \
  -num_containers "$NUM_CONTAINERS" \
  -container_memory "$CONTAINER_MEMORY" \
  -container_vcores 1 \
  -master_memory "$MASTER_MEMORY" \
  -timeout "$PROBE_TIMEOUT_MS" \
  -appname "$APP_NAME"
printf '    '; printf '%q ' "$@"; printf '\n'

if [ "$DRY_RUN" = "1" ]; then
  section "DRY_RUN=1 — the probe that WOULD have been shipped to each container"
  sed 's/^/    /' "$PROBE"
  section "DRY_RUN=1 — nothing was submitted"
  note "Re-run without DRY_RUN to submit. Confirm with the cluster owner first: this puts a"
  note "real application on a shared scheduler, briefly holding ${CONTAINER_MEMORY}MB × $NUM_CONTAINERS."
  exit 0
fi

OUT="$(mktemp "${TMPDIR:-/tmp}/csa-worker-probe-out.XXXXXX")"
section "3. Running (the client blocks until the application finishes)"
"$@" 2>&1 | tee "$OUT" | grep -E 'Submitted application|Application application_|appId|Final|FAILED|Exception' \
  | sed 's/^/    /'

APP="$(grep -oE 'application_[0-9]+_[0-9]+' "$OUT" | head -1)"
if [ -z "$APP" ]; then
  note "Could not find an applicationId in the client output. Full output: $OUT"
  fail "Submission did not yield an applicationId — read $OUT before re-running."
fi
note "applicationId: $APP"

# --- Collect the answers -----------------------------------------------------------------
# Log aggregation completes slightly after the app does, so the first `yarn logs` can come
# back empty on a cluster that has not finished uploading. Retry rather than concluding the
# probe produced nothing.
section "4. Aggregated container output"
LOGS=""
i=0
while [ "$i" -lt "$LOG_RETRIES" ]; do
  LOGS="$(yarn logs -applicationId "$APP" 2>/dev/null | grep -oE 'PROBE\[[^]]+\] .*')"
  [ -n "$LOGS" ] && break
  i=$(( i + 1 ))
  note "logs not aggregated yet (attempt $i/$LOG_RETRIES) — waiting ${LOG_RETRY_SLEEP}s"
  sleep "$LOG_RETRY_SLEEP"
done
if [ -z "$LOGS" ]; then
  note "No PROBE lines in the aggregated logs for $APP."
  note "Look yourself — the app may have failed before running the script:"
  note "  yarn logs -applicationId $APP | less"
  exit 2
fi
# sort -u is safe ONLY because every line carries its own hostname (gotcha 3).
printf '%s\n' "$LOGS" | sort -u | sed 's/^/    /'

# --- Verdict ------------------------------------------------------------------------------
section "SUMMARY"
HOSTS="$(printf '%s\n' "$LOGS" | sed -n 's/^PROBE\[\([^]]*\)\].*/\1/p' | sort -u)"
HOST_COUNT=$(printf '%s\n' "$HOSTS" | grep -c .)
RC=0
for h in $HOSTS; do
  V="$(printf '%s\n' "$LOGS" | sed -n "s/^PROBE\[$h\] VERDICT: //p" | sort -u | tr '\n' ' ')"
  printf '    %-28s %s\n' "$h" "${V:-no VERDICT line — read the raw output above}"
  case "$V" in *READY*) ;; *) RC=1 ;; esac
done
printf '\n'
note "nodes that answered: $HOST_COUNT of $NODE_COUNT RUNNING NodeManagers"
if [ "$HOST_COUNT" -lt "$NODE_COUNT" ]; then
  note "*** PARTIAL COVERAGE — this is NOT a pass. The unverified nodes are exactly where an"
  note "    intermittent failure would come from. Containers co-located instead of spreading;"
  note "    raise MEM_FRACTION (currently $MEM_FRACTION) or NUM_CONTAINERS and run it again."
  RC=1
fi
if [ "$RC" = "0" ]; then
  note "ALL NODES READY — the node-provided PyFlink stack is present and importable inside a"
  note "real container on every worker. That is what makes build_csa_venv_gateway.sh's"
  note "--system-site-packages venv shippable, and what makes retiring -pyarch safe here."
else
  note "NOT a clean pass. Until every worker reports READY, ship the interpreter explicitly"
  note "(-pyarch with the archive from build_csa_venv_gateway.sh) rather than relying on the"
  note "node's own stack — a job that avoids the bad node will pass and mislead you."
fi
exit "$RC"
