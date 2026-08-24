# Scaling Kubernetes Pods on Real Traffic with KEDA + Prometheus

This is a step-by-step guide to autoscaling a Kubernetes app on a real
traffic metric (**requests per second**) instead of CPU — using **KEDA**
this time, instead of the Prometheus Adapter + HPA combo. It's built for a
[Killercoda "Kubernetes Playground"](https://killercoda.com/playgrounds/scenario/kubernetes)
cluster, and it's the companion to the
[Prometheus Adapter version](../Prometheus-Adapter/Readme.md) of this same demo.

> ⚠️ **Note:** unlike the Adapter version of this demo, I haven't run every
> command below on a live cluster yet, so the outputs shown are **what you
> should expect to see**, not a copy-pasted transcript. The YAML and Helm
> values are correct against the docs linked at the bottom, but do a test
> run yourself before you rely on this for anything important.

**Install split:**
- **Prometheus** → Helm (`prometheus-community/prometheus`)
- **KEDA** → Helm (`kedacore/keda`)
- **Everything else** (demo app, ScaledObject, load generator) → raw `kubectl apply -f`

## Contents

- [Architecture](#architecture)
- [KEDA Operator Architecture (chronological)](#keda-operator-architecture-chronological)
  - [Failure modes: what if a component dies?](#failure-modes-what-if-a-component-dies)
- [Files](#files)
- [Why KEDA instead of the Prometheus Adapter?](#why-keda-instead-of-the-prometheus-adapter)
- [Understanding the key configuration](#understanding-the-key-configuration)
- [0. Prerequisites (Killercoda)](#0-prerequisites-killercoda)
- [1. Namespaces + Helm repos](#1-namespaces--helm-repos)
- [2. Deploy the demo app](#2-deploy-the-demo-app-raw-kubectl-apply)
- [3. Install Prometheus via Helm](#3-install-prometheus-via-helm)
- [4. Install KEDA via Helm](#4-install-keda-via-helm)
- [5. Deploy the ScaledObject](#5-deploy-the-scaledobject-raw-kubectl-apply)
- [6. Trigger a scale-out with the load generator](#6-trigger-a-scale-out-with-the-load-generator-raw-kubectl-apply)
- [What you should observe](#what-you-should-observe)
- [7. Cleanup](#7-cleanup)
- [Troubleshooting](#troubleshooting)
- [References](#references)

## Architecture

<img width="1533" height="1016" alt="image" src="https://github.com/user-attachments/assets/fd31eadf-91a1-4259-bc1d-675d23f28741" />


```mermaid
flowchart TD
    LG["load-generator<br/>(busybox Deployment)<br/><i>kubectl apply</i>"]

    subgraph demo["namespace: demo"]
        APP["demo-app<br/>(Deployment + Service)<br/>exposes /metrics<br/>http_requests_total counter"]
        SO["ScaledObject<br/><i>kubectl apply</i><br/>type: prometheus, threshold: 5"]
        HPA["keda-hpa-demo-app-scaledobject<br/><i>auto-created by KEDA</i>"]
    end

    subgraph monitoring["namespace: monitoring"]
        PROM["prometheus-server<br/><i>Helm release</i>"]
    end

    subgraph kedans["namespace: keda"]
        OPERATOR["keda-operator<br/><i>Helm release</i><br/>watches ScaledObjects"]
        SCALER["Prometheus scaler<br/><i>in-process module inside<br/>keda-operator, not its own Pod</i>"]
        METRICS["keda-operator-metrics-apiserver<br/>serves external.metrics.k8s.io"]
    end

    LG -->|"GET /work"| APP
    APP -->|"scraped every 15s<br/>prometheus.io/* annotations"| PROM
    SO -.->|"defines the trigger"| OPERATOR
    OPERATOR -->|"creates, on trigger 1"| SCALER
    OPERATOR -->|"creates, once"| HPA
    SCALER -->|"PromQL poll every 15s:<br/>sum(rate(http_requests_total[2m]))"| PROM
    SCALER -->|"caches the result in"| OPERATOR
    METRICS -->|"asks keda-operator over gRPC,<br/>serves external.metrics.k8s.io"| HPA
    HPA -->|"scales replicas"| APP
```

Same reason as the Adapter demo for using a *rate* instead of the raw
counter: `http_requests_total` only ever goes up, so if we scaled on that
number directly, it would scale up forever and never come back down. The
PromQL in the ScaledObject turns it into a `rate(...)` — a number that
rises and falls with actual traffic, like it should.

The big difference from the Adapter version: there's no
`custom.metrics.k8s.io` API to register, no adapter rule file to write,
and no RBAC to wire up by hand (`auth-delegator`, `auth-reader`,
`custom-metrics-server-resources`). KEDA's operator just talks to
Prometheus directly, and its own metrics server exposes
`external.metrics.k8s.io` to the HPA for you. More on that below in
[Why KEDA instead of the Prometheus Adapter?](#why-keda-instead-of-the-prometheus-adapter)

## KEDA Operator Architecture (chronological)

KEDA isn't a single process. Installing it actually gives you **three**
separate components plus a handful of CRDs — and on top of that, plain old
Kubernetes (the HPA controller inside `kube-controller-manager`, completely
untouched) still does the real scaling work. Here's who does what:

| Component | Runs as | Job |
|---|---|---|
| `keda-operator` | Deployment, `keda` ns | Watches your `ScaledObject`/`ScaledJob` resources, works out what needs scaling, runs the actual scalers (Prometheus, Kafka, etc.), and creates/owns the HPA for each ScaledObject. It also scales 0 ↔ 1 pods itself, since a normal HPA can't do anything when there are zero pods running. |
| ↳ **Prometheus scaler** | *A module living inside `keda-operator`* — **not** its own Pod | The part that actually speaks PromQL. One of these gets created per `type: prometheus` trigger, holding the `serverAddress`/`query`/`threshold` you configured. It's the **only** thing in this whole chain that ever talks to Prometheus. |
| `keda-operator-metrics-apiserver` | Deployment, `keda` ns | Exposes the `external.metrics.k8s.io` API that Kubernetes expects. It doesn't calculate anything itself — it just asks `keda-operator` for the number over an internal **gRPC** call (port `9666`) and passes back whatever it gets. |
| `keda-admission-webhooks` | Deployment, `keda` ns | Checks your `ScaledObject`/`ScaledJob` for obvious mistakes the moment you `kubectl apply` it (like `scaleTargetRef` missing, or `minReplicaCount` bigger than `maxReplicaCount`) — so you get an error right away instead of a config that silently never scales. |
| `ScaledObject` / `ScaledJob` CRDs | Custom Resource | The file you write (`manifests/30-scaledobject.yaml`). It says what to scale, the min/max replicas, and what should trigger scaling — here, a Prometheus query. |
| `TriggerAuthentication` / `ClusterTriggerAuthentication` CRDs | Custom Resource | Where you'd put credentials for a trigger that needs auth — API keys, DB passwords, etc. Not used here, since our Prometheus has no auth on it. |
| HPA controller | Inside `kube-controller-manager`, untouched | The thing that actually changes the replica count. KEDA doesn't replace it — it just feeds it a number through the external metrics API, the same way the Adapter demo fed it through the custom metrics API. |

> **The one thing worth remembering:** the **Prometheus scaler** (inside
> `keda-operator`) talks to *Prometheus*. The **HPA** talks to
> *Kubernetes' external metrics API*. `keda-operator-metrics-apiserver`
> just sits in between the two — and all three of those links can break
> on their own, independently of each other (see
> [Failure modes](#failure-modes-what-if-a-component-dies) below).

**What happens, in order** (steps ⑤–⑩ keep repeating on their own
schedules once the ScaledObject is up and running):

![KEDA + Prometheus chronological architecture](docs/keda-architecture.svg)

1. **You apply the ScaledObject** — `kubectl apply -f manifests/30-scaledobject.yaml`.
2. **`keda-admission-webhooks` checks it's valid** before Kubernetes even saves it.
3. **`keda-operator` picks it up** — works out that `demo-app` is what needs scaling, and creates a **Prometheus scaler** for this trigger using the `serverAddress`/`query`/`threshold` you gave it.
4. **`keda-operator` creates the HPA** for you — `keda-hpa-demo-app-scaledobject`, pointed at `demo-app`, wired to an External metric it owns.
5. **The Prometheus scaler runs the query** against Prometheus every `pollingInterval` (15s here), and caches the result plus an `isActive` flag. This is the only step where anything actually talks to Prometheus.
6. **Separately, the normal HPA controller does its own ~15s check**, sees the ScaledObject points at an External metric, and calls `external.metrics.k8s.io` to ask for it.
7. **`keda-operator-metrics-apiserver` answers that call** — it asks `keda-operator` for the cached value over gRPC, and hands it back.
8. **The HPA does the maths** — `desiredReplicas = ceil(totalValue / threshold)`, capped between `minReplicaCount` and `maxReplicaCount` — then updates `demo-app`'s replica count.
9. **Kubernetes creates or removes pods to match** — totally normal Kubernetes behaviour, nothing KEDA-specific about this step.
10. **The loop closes back through Prometheus** — the new set of pods changes what Prometheus scrapes, so the next PromQL check (back at step ⑤) reflects the new spread of load.

**The condensed version, if you just want the short list:**

| Step | Who | Talks to | What happens |
|---|---|---|---|
| 1 | Prometheus | `demo-app` | Scrapes `/metrics` every 15s |
| 2 | Prometheus scaler (in `keda-operator`) | Prometheus | Runs the PromQL query, every `pollingInterval` |
| 3 | `keda-operator` | `keda-operator-metrics-apiserver` | Hands over the cached value, over gRPC |
| 4 | HPA controller | `external.metrics.k8s.io` | Asks for the metric — has no idea Prometheus or PromQL even exist |
| 5 | HPA controller | `demo-app` Deployment | Changes `spec.replicas` |

**Scale-to-zero, for context (not used in this demo, since we keep
`minReplicaCount: 1`):** if the trigger stays quiet for `cooldownPeriod`,
`keda-operator` scales `demo-app` down to `0` itself — skipping the HPA
entirely, because an HPA can't do anything with zero pods (there'd be no
metrics to read). The moment traffic comes back, `keda-operator` bumps it
back up to `1`, then hands control back to the HPA for everything above
that. This is something the Adapter+HPA setup simply can't do — see the
[comparison table](#why-keda-instead-of-the-prometheus-adapter) below.

### Failure modes: what if a component dies?

There are three separate links in this chain, so Prometheus being healthy
doesn't mean scaling is working — and scaling working doesn't mean
Prometheus is healthy either. It's worth walking through what breaks when
each piece goes down:

**If `keda-operator-metrics-apiserver` crashes:**
```
Prometheus                        ✅  demo-app still gets scraped fine
Prometheus scaler                 ✅  keda-operator can still reach Prometheus
keda-operator-metrics-apiserver   ❌  external.metrics.k8s.io has no backend
HPA controller                    ✅  but its call for the External metric fails
```
So even though Prometheus and the scaler are both working fine, the HPA
has no way to get the number anymore. You'll see `kubectl describe hpa -n
demo` throw events like `unable to fetch metrics from external metrics
API`, and `kubectl get apiservice v1beta1.external.metrics.k8s.io` will
show `AVAILABLE: False`. To fix it: check `kubectl rollout status
deployment/keda-operator-metrics-apiserver -n keda` and look at its logs.

**If Prometheus goes down instead:**
```
Prometheus                        ❌  nothing to scrape from
Prometheus scaler                 ❌  its PromQL call to serverAddress fails
keda-operator-metrics-apiserver   ✅  still up, just has nothing fresh to serve
HPA controller                    ✅  still calling the API on schedule
```
`kubectl describe scaledobject demo-app-scaledobject -n demo` will show
`ScalingActive`/`Ready` flip to `False`, and `kubectl logs -n keda deploy/
keda-operator` will show the Prometheus scaler failing to connect
(connection refused / timeout). The HPA is still running fine — it just
can't get a fresh number. KEDA holds onto the last known value for a
little while, then marks the ScaledObject unhealthy.

The short version: the **scaler → Prometheus** link and the
**metrics-apiserver → HPA** link are two separate things that can break on
their own. If scaling stops working, the first question is always: which
one broke? `kubectl describe scaledobject` tells you about the first link,
`kubectl describe hpa` tells you about the second.

## Files

```
docs/
  keda-architecture.svg   chronological KEDA + Prometheus architecture diagram (see above)
helm/
  prometheus-values.yaml  Helm values for the Prometheus chart (adds the demo-app scrape job)
  keda-values.yaml        Helm values for the KEDA chart (trims resource requests/limits)
manifests/
  00-namespaces.yaml        demo + monitoring namespaces (keda namespace comes from --create-namespace)
  01-demo-app-configmap.yaml dummy app source (stdlib-only Python, no image build needed)
  02-demo-app-deployment.yaml Deployment + Service for the demo app
  30-scaledobject.yaml       ScaledObject targeting http_requests_total via a Prometheus trigger
  31-load-generator.yaml     busybox pod hammering demo-app to trigger a scale-out
```

No Dockerfile, no image registry needed — the "app" is the same small
Python script (stdlib only) from the Adapter demo, loaded in through a
`ConfigMap` and run using the public `python:3.12-alpine` image.

## Why KEDA instead of the Prometheus Adapter?

| | Prometheus Adapter + HPA | KEDA |
|---|---|---|
| Metric path | Prometheus → Adapter → `custom.metrics.k8s.io` → HPA | Prometheus → KEDA operator → HPA it creates for you |
| RBAC/API wiring | Manual: `APIService`, `auth-delegator`, `auth-reader`, `custom-metrics-server-resources` ClusterRoleBinding | Chart-managed; nothing to hand-wire |
| Config object | Adapter's `rules.custom` block (Helm values, cluster-wide) | `ScaledObject` CRD (per-workload, namespaced) |
| Scale to zero | Not possible — HPA has a hard `minReplicas: 1` floor | Supported (`minReplicaCount: 0` + `idleReplicaCount`) — not used in this demo to keep it directly comparable |
| Other event sources | Prometheus only | 60+ scalers (Kafka, SQS, RabbitMQ, cron, Redis, ...) behind the same CRD |
| Rule scope | One rule config governs every metric of that shape, cluster-wide | One ScaledObject per workload — easier to reason about, easier to blast-radius-limit |

Under the hood, both approaches still run on a normal Kubernetes
`HorizontalPodAutoscaler` — KEDA just creates and manages that HPA for you
(named `keda-hpa-<scaledobject-name>`) instead of you writing one by hand.

---

## Understanding the key configuration

Four things really matter here — three of them (app metrics, scrape
relabeling, HPA scaling maths) are exactly the same as the Adapter demo.

### 1. The app hand-writes its own metrics — no client library

```
# TYPE http_requests_total counter
http_requests_total{method="GET",path="/work"} 2
```

Same as before: plain text output, and `# TYPE counter` tells Prometheus
this number only ever goes up — which is exactly what `rate()` needs.

### 2. Scrape relabeling — the #1 silent-failure point

```yaml
kubernetes_sd_configs:
- role: pod
relabel_configs:
- source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
  action: keep
  regex: true
```

| Rule | Why |
|---|---|
| `role: pod` | Discovers **every** pod in the cluster as a candidate |
| `keep` on the annotation | Narrows it to pods with `prometheus.io/scrape: "true"` |
| rebuild `__address__` / `__metrics_path__` | Reads the actual port/path from `prometheus.io/port` and `/path` |
| `labelmap` | Copies pod labels onto the stored series |
| promote `namespace` / `pod` | Turns `__meta_kubernetes_namespace/pod_name` into plain labels the ScaledObject's PromQL can filter on |

### 3. The ScaledObject — one CRD, no separate rule config

```yaml
spec:
  scaleTargetRef:
    name: demo-app
  minReplicaCount: 1
  maxReplicaCount: 5
  pollingInterval: 15
  cooldownPeriod: 60
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus-server.monitoring.svc.cluster.local
      query: sum(rate(http_requests_total{namespace="demo"}[2m]))
      threshold: "5"
```

| Field | Job |
|---|---|
| `scaleTargetRef` | Which Deployment KEDA should scale — same idea as the HPA's `scaleTargetRef` |
| `pollingInterval` | How often, in seconds, KEDA re-runs the query. Separate from Prometheus's own 15s scrape interval. |
| `cooldownPeriod` | How long to wait after the trigger goes quiet before scaling back down to `minReplicaCount`. Mostly matters for scale-to-zero — we're only keeping it here to match the Adapter demo's cooldown behaviour. |
| `query` | The PromQL itself. No need for `by (pod)` like the Adapter version — KEDA's default `metricType` (`AverageValue`) automatically divides the total by however many replicas are running. |
| `threshold` | Same job as `averageValue: "5"` in the Adapter demo — the requests/sec we want per replica. |

### 4. No aggregated API RBAC to hand-wire

The Adapter demo needed you to wire up three separate RBAC bits by hand
(TLS trust, `auth-delegator`/`auth-reader`, and a ClusterRoleBinding)
because the Adapter itself *is* the `custom.metrics.k8s.io` API server.
KEDA's Helm chart sets all of that up for its own `external.metrics.k8s.io`
API — there's nothing left for you to configure. We check this is working
in step 4 below.

### 5. HPA scaling math (via the HPA KEDA creates)

Same maths as the Adapter demo, since KEDA's default `AverageValue`
metric type behaves exactly like normal Kubernetes HPA maths:

```
desiredReplicas = ceil(currentAverageValue / threshold)
```

So if 3 pods together are handling 30 req/s, that's `ceil(30 / 5)` = `6`
desired replicas, capped at our `maxReplicaCount: 5`. The
`stabilizationWindowSeconds: 60` setting stops it flapping up and down,
same as it did in the plain HPA version.

### 6. Load generator: concurrency, not rate, drives throughput

Same as the Adapter demo — 10 loops each firing one request at a time, so
the real rate is roughly `10 ÷ average latency`, not a flat "10 req/s".
More parallel loops means more throughput.

---

## 0. Prerequisites (Killercoda)

1. Start the **Kubernetes Playground** scenario on Killercoda (or any
   scenario that gives you a working multi-node cluster with `kubectl`
   pre-configured).
2. Confirm the cluster is up and Helm is available:
   ```bash
   kubectl get nodes
   helm version
   ```
   If `helm` isn't installed:
   ```bash
   curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
   chmod +x get_helm.sh
   ./get_helm.sh
   ```
3. Get this repo's `helm/` and `manifests/` directories onto the playground
   terminal (clone it if you've pushed it to GitHub, or copy the files over
   another way).

> **Heads up:** just like the Adapter demo, `kubectl top nodes` won't work
> on this cluster (`error: Metrics API not available`) — that needs a
> separate `metrics-server` component we're not installing here. Everything
> in this guide goes through `external.metrics.k8s.io` instead, served by
> KEDA's own metrics server (installed in step 4), a completely different API.

Everything below assumes your shell's current directory is this repo's
root (the one containing `helm/`, `manifests/`, `Readme.md`).

---

## 1. Namespaces + Helm repos

```bash
kubectl apply -f manifests/00-namespaces.yaml

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
```

Expected output:
```
namespace/demo created
namespace/monitoring created
"prometheus-community" has been added to your repositories
"kedacore" has been added to your repositories
Hang tight while we grab the latest from your chart repositories...
...Successfully got an update from the "prometheus-community" chart repository
...Successfully got an update from the "kedacore" chart repository
Update Complete. ⎈Happy Helming!⎈
```

The `keda` namespace isn't created here — `helm install --create-namespace`
in step 4 creates it for you.

---

## 2. Deploy the demo app (raw `kubectl apply`)

```bash
kubectl apply -f manifests/01-demo-app-configmap.yaml
kubectl apply -f manifests/02-demo-app-deployment.yaml
kubectl rollout status deployment/demo-app -n demo
```

**Verify the metric exists and increments** (identical to the Adapter demo):

```bash
kubectl run curl-test --rm -it --restart=Never --image=curlimages/curl -n demo \
  -- sh -c 'for i in $(seq 1 5); do curl -s http://demo-app-svc.demo.svc.cluster.local:8080/work; done; curl -s http://demo-app-svc.demo.svc.cluster.local:8080/metrics'
```

Expected output ends with:
```
# HELP http_requests_total Total number of HTTP requests handled by /work
# TYPE http_requests_total counter
http_requests_total{method="GET",path="/work"} 5
```

---

## 3. Install Prometheus via Helm

```bash
helm install prometheus prometheus-community/prometheus \
  -n monitoring \
  -f helm/prometheus-values.yaml

kubectl rollout status deployment/prometheus-server -n monitoring --timeout=180s
```

**Confirm the Service name/port** (the ScaledObject needs this to be right):

```bash
kubectl get svc -n monitoring
```

You should see `prometheus-server` on port `80`, matching `serverAddress`
in `manifests/30-scaledobject.yaml`. If your chart version names it
differently, update that field before step 5.

**Verify Prometheus is scraping the app** — same as the Adapter demo:

```bash
kubectl port-forward --address 0.0.0.0 -n monitoring svc/prometheus-server 9090:80
```

Open the Killercoda-exposed port `9090`, go to **Status → Targets**, and
confirm a `custom-pod-metrics` job with `demo-app`'s pod `UP`. Stop the
port-forward (`Ctrl+C`) once confirmed.

<img width="2551" height="1318" alt="image" src="https://github.com/user-attachments/assets/dc560577-721b-4895-a462-07c8baf4f686" />

---

## 4. Install KEDA via Helm

```bash
helm install keda kedacore/keda \
  -n keda --create-namespace \
  -f helm/keda-values.yaml

kubectl rollout status deployment/keda-operator -n keda --timeout=180s
kubectl rollout status deployment/keda-operator-metrics-apiserver -n keda --timeout=180s
```

**Verify the CRDs and pods:**

```bash
kubectl get crd | grep keda.sh
kubectl get pods -n keda
```

Expected:
```
scaledjobs.keda.sh
scaledobjects.keda.sh
triggerauthentications.keda.sh
clustertriggerauthentications.keda.sh

NAME                                                  READY   STATUS    RESTARTS   AGE
keda-admission-webhooks-xxxxxxxxxx-xxxxx              1/1     Running   0          60s
keda-operator-xxxxxxxxxx-xxxxx                        1/1     Running   0          60s
keda-operator-metrics-apiserver-xxxxxxxxxx-xxxxx      1/1     Running   0          60s
```

**Verify the external metrics APIService is healthy** — this is the KEDA
equivalent of the Adapter demo's `v1beta1.custom.metrics.k8s.io` check:

```bash
kubectl get apiservice v1beta1.external.metrics.k8s.io
```

You should see `AVAILABLE: True`. There's no separate cert/RBAC step to
check here — the chart already took care of it (see
[Understanding the key configuration, §4](#4-no-aggregated-api-rbac-to-hand-wire)).

---

## 5. Deploy the ScaledObject (raw `kubectl apply`)

```bash
kubectl apply -f manifests/30-scaledobject.yaml
kubectl get scaledobject -n demo
```

Expected:
```
NAME                     SCALETARGETKIND      SCALETARGETNAME   MIN   MAX   TRIGGERS     READY   ACTIVE   AGE
demo-app-scaledobject    apps/v1.Deployment    demo-app          1     5     prometheus   True    False    10s
```

`READY: True` means KEDA accepted the config and set up the trigger.
`ACTIVE: False` just means there's no load yet.

**Confirm KEDA created the underlying HPA:**

```bash
kubectl get hpa -n demo
```

Expected:
```
NAME                                    REFERENCE             TARGETS   MINPODS   MAXPODS   REPLICAS   AGE
keda-hpa-demo-app-scaledobject          Deployment/demo-app   0/5       1         5         1          15s
```

Give it a poll cycle (`pollingInterval: 15`) and then:

```bash
kubectl describe scaledobject demo-app-scaledobject -n demo
```

Look for `Condition Ready: True` and `Condition ScalingActive` — that
second one tells you KEDA is successfully querying Prometheus.

If it stays `False`/`Unknown` for more than ~2 minutes, see
[Troubleshooting](#troubleshooting).

---

## 6. Trigger a scale-out with the load generator (raw `kubectl apply`)

In one terminal, watch the HPA KEDA created:

```bash
kubectl get hpa -n demo -w
```

In another terminal, watch the pods:

```bash
kubectl get pods -n demo -w
```

In a third terminal, start the load generator (10 parallel tight loops
hitting `/work`, comfortably over the `5 req/s per pod` threshold):

```bash
kubectl apply -f manifests/31-load-generator.yaml
```

**Trigger scale-down:** delete the load generator and wait — the
`stabilizationWindowSeconds: 60` in the ScaledObject's
`advanced.horizontalPodAutoscalerConfig.behavior.scaleDown` means the HPA
waits ~60s of low load before scaling back down:

```bash
kubectl delete -f manifests/31-load-generator.yaml
kubectl get hpa -n demo -w
```

---

## What you should observe

Over 1-3 minutes after starting the load generator, expect the same shape
of behavior as the Adapter demo's real transcript (see that repo's
[Proof: watch it actually scale](../Prometheus-Adapter/Readme.md#proof-watch-it-actually-scale)
section for an actual captured run of the equivalent scenario):

1. `kubectl get hpa -n demo -w` shows `TARGETS` climbing well past `5`.
2. `demo-app`'s replica count follows, `Pending` → `Running`, up to
   `maxReplicaCount: 5`.
3. After deleting the load generator, the metric decays over ~2 minutes
   (`rate(...[2m])` is a rolling window), and replicas hold at `5` for
   ~60s past that (the `stabilizationWindowSeconds: 60`) before stepping
   back down to `minReplicaCount: 1`.

This all works the same way as the Adapter demo — the rolling-window
decay and the scale-down delay — because underneath, both are really just
the same `HorizontalPodAutoscaler` controller loop.

---

## 7. Cleanup

```bash
kubectl delete -f manifests/31-load-generator.yaml --ignore-not-found
kubectl delete -f manifests/30-scaledobject.yaml --ignore-not-found

helm uninstall keda -n keda
helm uninstall prometheus -n monitoring

kubectl delete -f manifests/02-demo-app-deployment.yaml --ignore-not-found
kubectl delete -f manifests/01-demo-app-configmap.yaml --ignore-not-found

kubectl delete namespace demo monitoring keda
```

---

## Troubleshooting

**Killercoda's Traffic Port Accessor shows `502 Bad Gateway`**
- Your `kubectl port-forward` is bound to `127.0.0.1` instead of `0.0.0.0`.
  Rerun with `--address 0.0.0.0`, e.g.
  `kubectl port-forward --address 0.0.0.0 -n monitoring svc/prometheus-server 9090:80`.

**`helm install` fails on an unrecognized key in `prometheus-values.yaml`**
- Chart versions occasionally rename subchart toggle keys. Run
  `helm show values prometheus-community/prometheus` to see current key
  names and adjust the values file.

**`kubectl get scaledobject` shows `READY: False` or `Unknown`**
- `kubectl describe scaledobject demo-app-scaledobject -n demo` — read the
  `Events` and `Conditions` blocks, they usually name the exact failure
  (e.g. can't reach `serverAddress`).
- `kubectl logs -n keda deploy/keda-operator` — look for errors reaching
  Prometheus or parsing the trigger query.
- Confirm `serverAddress` in `manifests/30-scaledobject.yaml` actually
  matches `kubectl get svc -n monitoring` (see step 3).

**`kubectl get apiservice v1beta1.external.metrics.k8s.io` shows `AVAILABLE: False`**
- `kubectl describe apiservice v1beta1.external.metrics.k8s.io` for the
  status message.
- `kubectl logs -n keda deploy/keda-operator-metrics-apiserver`.
- Confirm `kubectl rollout status deployment/keda-operator-metrics-apiserver -n keda`
  finished successfully.

**HPA shows `<unknown>` in `kubectl describe hpa -n demo`**
- Read the `Events` — same failure modes as the ScaledObject not-ready
  case above, since this HPA is entirely KEDA-managed.
- Confirm the series exists in Prometheus first:
  `curl 'http://localhost:9090/api/v1/query?query=http_requests_total'`
  via the port-forward from step 3. If empty, check **Status → Targets**
  in Prometheus for the `custom-pod-metrics` job and confirm `demo-app`'s
  pod carries `prometheus.io/scrape: "true"`
  (`kubectl get pod -n demo -o yaml | grep prometheus.io`).

**Load generator doesn't move the numbers**
- Confirm it's running cleanly: `kubectl logs -n demo deploy/load-generator`
  (should be silent — `wget -q` suppresses output on success).
- Confirm DNS resolves inside the cluster:
  `kubectl exec -n demo deploy/load-generator -- nslookup demo-app-svc.demo.svc.cluster.local`.
- Bump the loop count in `manifests/31-load-generator.yaml` (`seq 1 10` →
  higher) if 10 parallel loops aren't enough to clear the `5 req/s`
  threshold on your cluster's network latency.

**`prometheus-server` pod stuck `Pending`**
- Almost always a PVC waiting on a StorageClass that doesn't exist.
  Confirm `server.persistentVolume.enabled: false` is applied:
  `kubectl get pvc -n monitoring` should show nothing.

---

## References

- [KEDA — Deploying KEDA](https://keda.sh/docs/2.20/deploy/)
- [KEDA — Prometheus scaler](https://keda.sh/docs/2.20/scalers/prometheus/)
- [KEDA — ScaledObject specification](https://keda.sh/docs/2.20/reference/scaledobject-spec/)
- [KEDA — Scaling Deployments, StatefulSets & Custom Resources](https://keda.sh/docs/2.20/concepts/scaling-deployments/)
- [kedacore/charts on GitHub](https://github.com/kedacore/charts)
