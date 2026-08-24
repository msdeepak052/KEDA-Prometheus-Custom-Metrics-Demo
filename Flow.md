# KEDA + Prometheus: End-to-End

Assume we have:

```text
Application: order-service
Metric:      HTTP requests/sec
Goal:        Scale when requests > 100/sec
```

---

## 1. Application exposes metrics

Your application exposes something like:

```text
/metrics
```

Prometheus scrapes it.

```text
┌──────────────────┐
│  order-service   │
│                  │
│ /metrics         │
│                  │
│ requests = 250   │
└────────┬─────────┘
         │
         │ scrape
         ▼
┌──────────────────┐
│    Prometheus    │
│                  │
│ Stores metrics   │
└──────────────────┘
```

Prometheus might have:

```text
http_requests_total{app="order-service"}
```

---

# 2. You create a KEDA ScaledObject

For example:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: order-service
spec:
  scaleTargetRef:
    name: order-service

  minReplicaCount: 1
  maxReplicaCount: 10

  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: |
          sum(rate(http_requests_total{app="order-service"}[2m]))
        threshold: "100"
```

The important part is:

```yaml
type: prometheus
```

This tells KEDA:

> "Use the Prometheus scaler."

---

# 3. KEDA Operator watches the ScaledObject

The KEDA operator is watching Kubernetes resources.

It sees:

```text
ScaledObject
     │
     ▼
KEDA Operator
```

It understands:

```text
Trigger = Prometheus
Prometheus URL = ...
Query = ...
Threshold = 100
```

The operator then creates/manages an HPA for the workload.

You can see it with:

```bash
kubectl get hpa
```

You may see something like:

```text
NAME                         TARGETS
keda-hpa-order-service       250/100
```

---

# 4. Now KEDA's Prometheus scaler talks to Prometheus

This is the part you correctly identified.

The **Prometheus scaler** executes the configured PromQL query against Prometheus.

Conceptually:

```text
KEDA Prometheus scaler
        │
        │ PromQL
        ▼
   Prometheus
        │
        │ result
        ▼
      250
```

For example:

```promql
sum(rate(http_requests_total{app="order-service"}[2m]))
```

Prometheus returns:

```text
250
```

So KEDA knows:

```text
Current metric = 250
```

---

# 5. Now comes `keda-operator-metrics-apiserver`

This is where many people get confused.

The Prometheus scaler has obtained:

```text
250
```

But the **HPA needs to consume the metric through Kubernetes' external metrics API**.

That's where:

```text
keda-operator-metrics-apiserver
```

comes into the picture.

It exposes KEDA's metrics through:

```text
external.metrics.k8s.io
```

Conceptually:

```text
                  KEDA
                   │
                   │ metric = 250
                   ▼
       ┌─────────────────────────┐
       │ KEDA Metrics API Server │
       │                         │
       │ external.metrics.k8s.io │
       └────────────┬────────────┘
                    │
                    ▼
                   HPA
```

---

# 6. HPA asks for the metric

The HPA doesn't need to know:

```text
Where is Prometheus?
What is PromQL?
How do I authenticate to Prometheus?
```

It simply works through Kubernetes' metrics API.

Conceptually:

```text
HPA
 │
 │ "Give me the external metric"
 ▼
external.metrics.k8s.io
 │
 ▼
KEDA Metrics API Server
 │
 ▼
metric = 250
```

So the HPA gets:

```text
250
```

---

# 7. HPA calculates desired replicas

Your threshold is:

```text
100 requests/sec
```

Current value:

```text
250 requests/sec
```

So HPA determines that more replicas are required.

Conceptually:

```text
Current metric = 250
Target metric  = 100

250 / 100 = 2.5
```

If you're currently running:

```text
1 pod
```

HPA can scale toward approximately:

```text
3 pods
```

subject to HPA's actual scaling calculations and limits.

---

# 8. Deployment gets scaled

Now Kubernetes changes the Deployment:

```text
Before:

order-service
├── Pod 1


After:

order-service
├── Pod 1
├── Pod 2
└── Pod 3
```

Traffic is distributed across the additional pods.

---

# Complete flow

Now put everything together:

```text
                         EKS
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  ┌──────────────────┐                                      │
│  │  order-service   │                                      │
│  │                  │                                      │
│  │  /metrics        │                                      │
│  └────────┬─────────┘                                      │
│           │                                                 │
│           │ ① Scrape                                       │
│           ▼                                                 │
│  ┌──────────────────┐                                      │
│  │    Prometheus    │                                      │
│  │                  │                                      │
│  │ http_requests... │                                      │
│  └────────▲─────────┘                                      │
│           │                                                 │
│           │ ② PromQL                                       │
│           │                                                 │
│  ┌────────┴─────────┐                                      │
│  │ KEDA Operator    │                                      │
│  │                  │                                      │
│  │ Prometheus       │                                      │
│  │ Scaler           │                                      │
│  └────────┬─────────┘                                      │
│           │                                                 │
│           │ ③ metric = 250                                  │
│           ▼                                                 │
│  ┌────────────────────────────┐                            │
│  │ keda-operator-metrics-     │                            │
│  │ apiserver                  │                            │
│  │                            │                            │
│  │ external.metrics.k8s.io    │                            │
│  └────────────┬───────────────┘                            │
│               │                                             │
│               │ ④ external metric                          │
│               ▼                                             │
│          ┌────────────┐                                    │
│          │    HPA     │                                    │
│          │            │                                    │
│          │ target=100 │                                    │
│          └─────┬──────┘                                    │
│                │                                            │
│                │ ⑤ scale                                   │
│                ▼                                            │
│        ┌─────────────────┐                                 │
│        │   Deployment    │                                 │
│        │                 │                                 │
│        │ Pod Pod Pod     │                                 │
│        └─────────────────┘                                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## The 5 interactions to remember

| Step | Who                    | Talks to             | What happens                |
| ---- | ---------------------- | -------------------- | --------------------------- |
| 1    | Prometheus             | Application          | Scrapes `/metrics`          |
| 2    | KEDA Prometheus scaler | Prometheus           | Executes PromQL             |
| 3    | KEDA                   | Metrics API Server   | Makes KEDA metric available |
| 4    | HPA                    | External Metrics API | Gets metric                 |
| 5    | HPA                    | Deployment           | Changes replica count       |

---

# Now what if Metrics API Server dies?

This is the important operational scenario.

Suppose:

```text
Prometheus                     ✅
KEDA Operator                  ✅
Prometheus scaler              ✅
KEDA Metrics API Server        ❌
HPA                            ✅
```

Prometheus continues working.

KEDA can still potentially query Prometheus.

For example:

```text
Prometheus
    ▲
    │
    │ 250
    │
KEDA Prometheus scaler
```

But the HPA's path to the external metric is broken:

```text
HPA
 │
 │ "Give me external metric"
 ▼
external.metrics.k8s.io
 │
 X
 │
 ❌ KEDA Metrics API Server
```

Therefore the HPA can't reliably obtain the KEDA-provided external metric.

You may see:

```bash
kubectl describe hpa
```

showing errors such as:

```text
failed to get external metric
unable to fetch metrics
```

The key distinction is:

> **Prometheus being healthy does not guarantee KEDA-based HPA scaling will work.**

---

# What if Prometheus dies instead?

Now reverse it:

```text
Prometheus                     ❌
KEDA Operator                  ✅
Metrics API Server             ✅
HPA                            ✅
```

The Prometheus scaler can't obtain the metric:

```text
KEDA Prometheus scaler
        │
        │ PromQL
        ▼
   Prometheus
        X
        ❌
```

Therefore KEDA doesn't have the fresh Prometheus metric needed for that trigger.

So:

```text
Prometheus down
      ↓
KEDA can't query metric
      ↓
Metric unavailable
      ↓
HPA scaling decision affected
```

---

# And one final distinction: KEDA Operator vs Metrics API Server

Think of them like this:

```text
┌──────────────────────────────────────────────┐
│                KEDA Operator                 │
│                                              │
│  "I manage the ScaledObject and scalers."    │
│                                              │
│  Prometheus scaler                           │
│  Kafka scaler                                │
│  SQS scaler                                  │
│  etc.                                        │
└──────────────────────┬───────────────────────┘
                       │
                       │ metric
                       ▼
┌──────────────────────────────────────────────┐
│       KEDA Operator Metrics API Server       │
│                                              │
│ "I'll expose KEDA metrics through the        │
│  Kubernetes external metrics API."           │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
                    HPA
```

### The interview answer

If someone asks:

> **"How does KEDA scale a deployment using Prometheus?"**

A strong answer is:

> **"KEDA's Prometheus scaler queries Prometheus using the PromQL configured in the ScaledObject. KEDA then makes the resulting metric available through the Kubernetes external metrics API via its metrics API server. The HPA consumes that external metric, calculates the desired replica count, and scales the target Deployment."**

And the critical distinction:

> **"The Prometheus scaler talks to Prometheus; the HPA talks to the Kubernetes external metrics API. `keda-operator-metrics-apiserver` is the bridge between KEDA's metrics and that Kubernetes API path."**
