# KEDA + Prometheus: End-to-End

---

# 1. Our exact example

We will use this topology throughout:

```text
monitoring namespace
└── Prometheus
    └── Service: prometheus
        └── port 9090


keda namespace
├── keda-operator
├── keda-operator-metrics-apiserver
└── keda-admission-webhooks


demo namespace
├── Deployment: order-service
├── Service: order-service
├── ScaledObject: order-service
└── HPA: keda-hpa-order-service
```

Our requirement:

> Scale `order-service` based on a Prometheus metric.

Let's say:

```text
Prometheus metric:

http_requests_total

PromQL:

sum(rate(http_requests_total{app="order-service"}[2m]))

Target:

100 requests/sec
```

And:

```text
min replicas = 1
max replicas = 10
```

---

# 2. First understand the KEDA components

A KEDA installation is **not just one pod**.

The important components for this discussion are:

```text
                   KEDA
                    │
       ┌────────────┼─────────────┐
       │            │             │
       ▼            ▼             ▼
   Operator    Metrics API    Admission
                Server         Webhooks
```

They have very different responsibilities.

---

# 3. KEDA Operator

This is the **brain/controller**.

```text
keda
└── keda-operator
```

It watches resources such as:

```text
ScaledObject
ScaledJob
TriggerAuthentication
ClusterTriggerAuthentication
```

For our example:

```text
demo
└── ScaledObject/order-service
```

The Operator notices it.

It then:

* resolves the configured scaler
* creates/manages the scaler
* determines metric specifications
* creates/manages the HPA
* performs KEDA's activation/scaling logic
* maintains scaler caches
* handles authentication configuration
* obtains metrics from scalers
* handles fallback/error behavior

KEDA's current `ScaleHandler` is the central piece responsible for invoking scalers, getting metric specifications, retrieving metrics, and making scaling decisions. ([GitHub][1])

So think:

```text
KEDA Operator
=
controller + scaler manager + scaling brain
```

---

# 4. KEDA Scaler

The scaler is **not a separate Kubernetes Pod** for every trigger.

This is an important distinction.

For:

```yaml
triggers:
  - type: prometheus
```

KEDA loads/creates the **Prometheus scaler implementation inside the Operator**.

Conceptually:

```text
KEDA Operator
│
├── Prometheus scaler
├── Kafka scaler
├── AWS SQS scaler
├── Azure scaler
├── Redis scaler
└── ...
```

For our example:

```text
KEDA Operator
      │
      └── Prometheus Scaler
```

The Prometheus scaler knows how to:

1. connect to Prometheus
2. execute the PromQL
3. parse the response
4. return the metric value
5. determine whether the trigger is active

The current KEDA Prometheus scaler source shows `GetMetricsAndActivity()` executing the Prometheus query and returning the resulting external metric. ([GitHub][2])

---

# 5. Metrics API Server

Now the second major component:

```text
keda
└── keda-operator-metrics-apiserver
```

Its job is **not** to independently query Prometheus.

Its job is:

> Make KEDA's metrics available through Kubernetes' external metrics API.

The API is:

```text
external.metrics.k8s.io
```

So:

```text
HPA
 │
 │ external metric request
 ▼
Kubernetes API
 │
 ▼
KEDA Metrics API Server
```

And internally:

```text
Metrics API Server
       │
       │ gRPC
       ▼
KEDA Operator
```

This is confirmed directly by KEDA's current source: the gRPC `GetMetrics()` implementation calls `GetScaledObjectMetrics()` on the operator's `ScaleHandler`. ([GitHub][3])

---

# 6. Admission Webhooks

The third KEDA component:

```text
keda
└── keda-admission-webhooks
```

This is **not part of the metric retrieval path**.

Its responsibility is validation/admission.

For example, KEDA can validate:

```text
ScaledObject
ScaledJob
TriggerAuthentication
```

and perform validation around HPA ownership.

KEDA documents the admission webhook and its validation behavior separately. ([KEDA][4])

So don't put it into the Prometheus → HPA metric path.

The path is:

```text
Operator
Metrics API Server
HPA
```

not:

```text
Operator
  ↓
Webhook
  ↓
Metrics
```

---

# 7. Now create the Deployment

In `demo`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
  namespace: demo

spec:
  replicas: 1

  selector:
    matchLabels:
      app: order-service

  template:
    metadata:
      labels:
        app: order-service

    spec:
      containers:
      - name: order-service
        image: nginx
```

Initially:

```text
demo

Deployment/order-service
        │
        └── Pod
```

---

# 8. Prometheus is somewhere else

Our Prometheus is:

```text
monitoring
└── prometheus
```

with:

```text
Service:

prometheus
```

Kubernetes DNS gives us:

```text
prometheus.monitoring.svc.cluster.local
```

Therefore:

```text
http://prometheus.monitoring.svc.cluster.local:9090
```

is the address KEDA can use.

Notice:

```text
KEDA = keda namespace

Application = demo namespace

Prometheus = monitoring namespace
```

There is absolutely no requirement that they be in the same namespace.

---

# 9. How does KEDA know Prometheus's location?

Through the `ScaledObject`.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject

metadata:
  name: order-service
  namespace: demo

spec:

  scaleTargetRef:
    name: order-service

  minReplicaCount: 1
  maxReplicaCount: 10

  triggers:

  - type: prometheus

    metadata:

      serverAddress: http://prometheus.monitoring.svc.cluster.local:9090

      query: |
        sum(rate(http_requests_total{app="order-service"}[2m]))

      threshold: "100"
```

This gives KEDA:

```text
Scaler type
    ↓
Prometheus

Prometheus endpoint
    ↓
prometheus.monitoring.svc.cluster.local:9090

PromQL
    ↓
sum(rate(...))

Target
    ↓
100
```

---

# 10. The Operator reads this

The KEDA Operator watches:

```text
demo/ScaledObject/order-service
```

It sees:

```text
trigger type = prometheus
```

Therefore it creates the Prometheus scaler.

Conceptually:

```text
ScaledObject
     │
     │
     ▼
KEDA Operator
     │
     │ "This is a Prometheus trigger"
     ▼
Prometheus Scaler
```

---

# 11. How does the Prometheus scaler connect?

This answers your earlier question.

The scaler uses:

```yaml
serverAddress:
  http://prometheus.monitoring.svc.cluster.local:9090
```

So the network path is:

```text
keda namespace

KEDA Operator
      │
      │ HTTP
      │
      │ DNS:
      │ prometheus.monitoring.svc.cluster.local
      ▼

monitoring namespace

Prometheus Service :9090
      │
      ▼
Prometheus Pod
```

Kubernetes DNS resolves the Service.

So **namespace is encoded in the DNS name**.

---

# 12. KEDA sends PromQL

The Prometheus scaler executes:

```promql
sum(rate(http_requests_total{app="order-service"}[2m]))
```

against Prometheus's HTTP query API.

Conceptually:

```text
KEDA Prometheus Scaler
        │
        │ HTTP GET /api/v1/query
        │
        │ query=<PromQL>
        ▼
Prometheus
```

Prometheus evaluates it.

Suppose the answer is:

```text
250
```

Therefore:

```text
Prometheus Scaler

metric = 250
```

---

# 13. But we haven't involved HPA yet

Correct.

Now KEDA creates the HPA.

KEDA's ScaledObject reconciler creates/manages an HPA named by default:

```text
keda-hpa-order-service
```

in:

```text
demo
```

You can verify:

```bash
kubectl get hpa -n demo
```

KEDA documents this naming convention and HPA ownership behavior. ([KEDA][4])

---

# 14. Conceptual generated HPA

It will look conceptually like:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler

metadata:
  name: keda-hpa-order-service
  namespace: demo

spec:

  minReplicas: 1
  maxReplicas: 10

  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: order-service

  metrics:

  - type: External

    external:

      metric:
        name: <KEDA metric>

        selector:
          matchLabels:
            scaledobject.keda.sh/name: order-service

      target:
        type: AverageValue
        averageValue: "100"
```

**The exact generated metric name can vary**, so use:

```bash
kubectl get hpa keda-hpa-order-service -n demo -o yaml
```

to see your real one.

KEDA's scaler interface provides the metric specification that is used to construct the HPA, and KEDA uses `AverageValue` for external metrics. ([GitHub][5])

---

# 15. Why `External`?

Because:

```text
CPU
Memory
```

are Kubernetes resource metrics.

But:

```text
Prometheus HTTP request rate
Kafka lag
AWS SQS queue length
Redis queue length
```

are **external metrics** from KEDA's point of view.

Therefore:

```yaml
type: External
```

---

# 16. Now the HPA wants the metric

The HPA doesn't know anything about:

```text
Prometheus
```

It knows:

```text
I need metric X.
```

So it asks Kubernetes for:

```text
external.metrics.k8s.io
```

Conceptually:

```text
HPA
 │
 │ "Give me metric X"
 ▼
Kubernetes API Server
```

---

# 17. API Aggregation

Kubernetes has an API aggregation mechanism.

KEDA registers:

```text
v1beta1.external.metrics.k8s.io
```

You can inspect:

```bash
kubectl get apiservice | grep external.metrics
```

Conceptually:

```text
HPA
 │
 ▼
Kubernetes API Server
 │
 │ API aggregation
 ▼
KEDA Metrics API Server
```

This is why the Metrics API Server is necessary.

---

# 18. Now the crucial internal KEDA hop

The Metrics API Server receives:

```text
"Give me the metric for demo/order-service"
```

It does **not** independently query Prometheus.

Instead:

```text
KEDA Metrics API Server
          │
          │ gRPC
          ▼
KEDA Operator
```

KEDA's current implementation is very explicit:

```text
Metrics Server gRPC GetMetrics()
                ↓
ScaleHandler.GetScaledObjectMetrics()
```

The source code shows exactly this. ([GitHub][3])

---

# 19. Why does Metrics Server ask the Operator?

Because the Operator owns the scaler state/configuration.

The Operator knows:

```text
ScaledObject:
    demo/order-service

Trigger:
    prometheus

Prometheus:
    prometheus.monitoring.svc.cluster.local:9090

Query:
    sum(rate(...))

Authentication:
    whatever TriggerAuthentication specifies

Metric:
    KEDA-generated metric
```

The Metrics API Server shouldn't have to recreate all of that.

So:

```text
Metrics API Server
        │
        │ "Operator, give me metric X"
        ▼
KEDA Operator
```

---

# 20. Operator finds the ScaledObject

The Operator receives approximately:

```text
namespace:
    demo

ScaledObject:
    order-service

metric:
    X
```

It finds:

```text
demo/ScaledObject/order-service
```

Then it finds the scaler:

```text
Prometheus scaler
```

KEDA maintains scaler caches for ScaledObjects, and its current `GetScaledObjectMetrics()` implementation retrieves the relevant scaler/cache and invokes the scaler's metric retrieval path. ([GitHub][1])

---

# 21. Operator asks Prometheus scaler

Now we come back to the first path:

```text
KEDA Operator
       │
       ▼
Prometheus Scaler
       │
       │ PromQL
       ▼
Prometheus
```

Prometheus returns:

```text
250
```

The scaler returns that metric to the Operator.

---

# 22. Operator → Metrics API Server

The Operator sends the result back through gRPC:

```text
Prometheus
    │
    │ 250
    ▼
Prometheus Scaler
    │
    ▼
KEDA Operator
    │
    │ gRPC response
    ▼
KEDA Metrics API Server
```

The Metrics API Server converts it into the Kubernetes external metric representation.

---

# 23. Metrics API Server → Kubernetes API → HPA

Then:

```text
KEDA Metrics API Server
       │
       ▼
Kubernetes API Server
       │
       ▼
HPA
```

HPA finally gets:

```text
current metric = 250
```

---

# 24. HPA calculates desired replicas

Target:

```text
100
```

Current:

```text
250
```

Suppose currently:

```text
2 replicas
```

Conceptually:

```text
desired replicas
=
2 × 250 / 100

= 5
```

So HPA decides:

```text
desired replicas = 5
```

---

# 25. HPA scales Deployment

HPA changes the Deployment's scale:

```text
HPA
 │
 │ replicas = 5
 ▼
Deployment/order-service
```

Now:

```text
order-service

Pod 1
Pod 2
Pod 3
Pod 4
Pod 5
```

---

# 26. Now the entire request/response cycle

## Request

```text
HPA
 │
 │ ① external metric request
 ▼
Kubernetes API Server
 │
 │ ② API aggregation
 ▼
KEDA Metrics API Server
 │
 │ ③ gRPC GetMetrics()
 ▼
KEDA Operator
 │
 │ ④ find ScaledObject
 ▼
Prometheus Scaler
 │
 │ ⑤ PromQL
 ▼
Prometheus
```

## Response

```text
Prometheus
 │
 │ ⑥ metric = 250
 ▼
Prometheus Scaler
 │
 ▼
KEDA Operator
 │
 │ ⑦ gRPC response
 ▼
KEDA Metrics API Server
 │
 ▼
Kubernetes API Server
 │
 ▼
HPA
```

## Scaling

```text
HPA
 │
 │ ⑧ calculate desired replicas
 ▼
Deployment
 │
 │ ⑨ create/delete Pods
 ▼
Pods
```

---

# 27. But KEDA has another scaling loop

This is another thing we need to keep.

KEDA is not simply an HPA metric adapter.

KEDA itself has **activation logic**.

For example:

```text
minReplicaCount = 0
```

Suppose Prometheus says:

```text
0 requests
```

KEDA can determine that the scaler is inactive.

Then:

```text
0 replicas
```

Later Prometheus says:

```text
250 requests
```

KEDA determines:

```text
trigger active
```

and activates the workload.

The scaler interface includes:

```text
IsActive()
GetMetricSpec()
GetMetrics()
Close()
```

and KEDA uses the scaler's activity result to determine activation. ([GitHub][5])

So there are really **two concepts**:

```text
ACTIVATION

"Should this workload be running?"

        ↓

SCALING

"How many replicas should it have?"
```

---

# 28. Example: scale from 0 → 3

Suppose:

```text
minReplicaCount = 0
maxReplicaCount = 10
```

Prometheus:

```text
requests = 0
```

KEDA:

```text
inactive
```

Deployment:

```text
0 Pods
```

Then traffic starts.

Prometheus:

```text
requests = 300
```

KEDA Prometheus scaler:

```text
IsActive = true
```

KEDA activates the workload.

Then the HPA can scale based on:

```text
300 / 100
```

Eventually:

```text
3 Pods
```

This is one of the key differences between plain HPA and KEDA.

---

# 29. TriggerAuthentication

Now let's add another KEDA component that matters in real environments.

Suppose Prometheus requires authentication.

Don't put credentials directly in:

```yaml
ScaledObject
```

Instead, KEDA supports:

```text
TriggerAuthentication
```

For example:

```yaml
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication

metadata:
  name: prometheus-auth
  namespace: demo

spec:
  secretTargetRef:
  - parameter: bearerToken
    name: prometheus-secret
    key: token
```

Then:

```yaml
triggers:
- type: prometheus

  metadata:
    serverAddress: https://prometheus.monitoring.svc.cluster.local:9090
    query: ...
    threshold: "100"

  authenticationRef:
    name: prometheus-auth
```

Now the architecture becomes:

```text
KEDA Operator
    │
    ├── ScaledObject
    │
    ├── TriggerAuthentication
    │
    └── Prometheus Scaler
             │
             │ authenticated request
             ▼
         Prometheus
```

KEDA's scaler infrastructure resolves authentication information when building the scaler configuration. The current `ScaleHandler` includes the authentication client set and scaler-cache construction for this purpose. ([GitHub][1])

---

# 30. What if Prometheus is protected by NetworkPolicy?

Now namespaces matter operationally.

You might have:

```text
keda namespace
       │
       │ NetworkPolicy
       X
       │
monitoring namespace
```

Then your configuration may be perfectly correct:

```text
serverAddress = correct
PromQL = correct
```

but:

```text
KEDA Operator
      X
Prometheus
```

because network traffic is blocked.

So troubleshooting must include:

```text
DNS
Service
Port
NetworkPolicy
TLS
Authentication
PromQL
```

---

# 31. What happens if Metrics API Server dies?

Now the architecture tells us exactly what happens:

```text
HPA
 │
 ▼
Kubernetes API Server
 │
 X
 ▼
KEDA Metrics API Server ❌
```

Therefore:

```text
HPA cannot obtain KEDA external metric
```

Even if:

```text
Prometheus       ✅
KEDA Operator    ✅
Prometheus scaler ✅
```

are healthy.

---

# 32. What happens if KEDA Operator dies?

```text
HPA
 │
 ▼
Metrics API Server
 │
 X
 ▼
KEDA Operator ❌
```

The Metrics API Server cannot get the metric from the Operator.

The current KEDA architecture explicitly uses the Operator's gRPC metrics service for this retrieval. ([GitHub][3])

---

# 33. What happens if Prometheus dies?

```text
HPA
 │
 ▼
Metrics API Server
 │
 ▼
KEDA Operator
 │
 ▼
Prometheus Scaler
 │
 X
 ▼
Prometheus ❌
```

Metric retrieval fails.

---

# 34. What happens if the ScaledObject is wrong?

For example:

```yaml
serverAddress: http://wrong-prometheus:9090
```

Then:

```text
KEDA Operator
      │
      ▼
Prometheus Scaler
      │
      X
      ▼
wrong-prometheus
```

Metric retrieval fails.

---

# 35. The three KEDA pods — what they actually do

This is worth memorizing:

| KEDA component                      | Main responsibility                                                                          |
| ----------------------------------- | -------------------------------------------------------------------------------------------- |
| **keda-operator**                   | Watches KEDA resources, manages scalers/HPA, performs scaling/activation logic               |
| **keda-operator-metrics-apiserver** | Serves KEDA metrics through `external.metrics.k8s.io` and gets values from Operator via gRPC |
| **keda-admission-webhooks**         | Validates/admission-controls KEDA resources and HPA ownership                                |

The Metrics API Server itself exposes Prometheus metrics about its internal gRPC activity as well, so it can itself be monitored. ([KEDA][6])

---

# 36. The most important separation

You should now mentally separate these three:

### A. Prometheus connection

```text
KEDA Operator
     │
     │ Prometheus scaler
     │ HTTP/PromQL
     ▼
Prometheus
```

### B. Kubernetes metric connection

```text
HPA
 │
 ▼
Kubernetes API Server
 │
 ▼
Metrics API Server
```

### C. Internal KEDA metric connection

```text
Metrics API Server
       │
       │ gRPC
       ▼
KEDA Operator
       │
       ▼
Prometheus Scaler
       │
       ▼
Prometheus
```

Put together:

```text
                         monitoring
                     ┌───────────────┐
                     │  Prometheus   │
                     │     :9090     │
                     └──────▲────────┘
                            │
                     HTTP/PromQL
                            │
                            │
                         keda
                     ┌──────┴────────┐
                     │ KEDA Operator │
                     │               │
                     │ Prometheus     │
                     │ Scaler        │
                     └──────▲────────┘
                            │
                          gRPC
                            │
                     ┌──────┴──────────────┐
                     │ Metrics API Server  │
                     └──────▲──────────────┘
                            │
                  external.metrics.k8s.io
                            │
                     ┌──────┴──────────────┐
                     │ Kubernetes API      │
                     │ Server              │
                     │ Aggregation         │
                     └──────▲──────────────┘
                            │
                          demo
                     ┌──────┴──────────────┐
                     │ HPA                 │
                     │                     │
                     │ target = 100        │
                     └──────┬──────────────┘
                            │
                            │ replicas
                            ▼
                     ┌──────────────┐
                     │ Deployment   │
                     │ order-service│
                     └──────────────┘
```

---

# 37. Finally: who talks to whom?

This is the **cheat sheet I'd use in an interview**:

```text
1. Application
      ↓
   exposes /metrics

2. Prometheus
      ↓
   scrapes application

3. ScaledObject
      ↓
   tells KEDA:
   "Use Prometheus scaler"

4. KEDA Operator
      ↓
   creates/configures Prometheus scaler

5. Prometheus Scaler
      ↓
   queries:
   prometheus.monitoring.svc.cluster.local:9090

6. KEDA Operator
      ↓
   creates/manages HPA

7. HPA
      ↓
   asks Kubernetes:
   "Give me external metric"

8. Kubernetes API Server
      ↓
   API aggregation

9. KEDA Metrics API Server
      ↓
   gRPC

10. KEDA Operator
       ↓
    finds ScaledObject
       ↓
    finds Prometheus scaler

11. Prometheus Scaler
       ↓
    executes PromQL

12. Prometheus
       ↓
    returns 250

13. Value travels back:
    Prometheus
       ↓
    Scaler
       ↓
    Operator
       ↓
    Metrics API Server
       ↓
    Kubernetes API
       ↓
    HPA

14. HPA
       ↓
    calculates replicas

15. Deployment
       ↓
    creates/deletes Pods
```

**That's the complete mental model.**

The single most important thing to remember is that **the Prometheus scaler lives logically under the KEDA Operator; it is the scaler that knows how to talk to Prometheus. The Metrics API Server is a Kubernetes-facing adapter, and its internal gRPC path goes back to the KEDA Operator to obtain the metric.** KEDA's current source makes this relationship explicit. ([GitHub][1])

[1]: https://github.com/kedacore/keda/blob/main/pkg/scaling/scale_handler.go?utm_source=chatgpt.com "keda/pkg/scaling/scale_handler.go at main · kedacore/keda · GitHub"
[2]: https://github.com/kedacore/keda/blob/main/pkg/scalers/prometheus_scaler.go?utm_source=chatgpt.com "keda/pkg/scalers/prometheus_scaler.go at main · kedacore/keda · GitHub"
[3]: https://github.com/kedacore/keda/blob/main/pkg/metricsservice/server.go?utm_source=chatgpt.com "keda/pkg/metricsservice/server.go at main · kedacore/keda · GitHub"
[4]: https://keda.sh/docs/2.20/operate/admission-webhooks/?utm_source=chatgpt.com "Admission Webhooks | KEDA"
[5]: https://github.com/kedacore/keda-docs/blob/main/content/docs/2.5/concepts/external-scalers.md?utm_source=chatgpt.com "keda-docs/content/docs/2.5/concepts/external-scalers.md at main · kedacore/keda-docs · GitHub"
[6]: https://keda.sh/docs/2.20/integrations/prometheus/?utm_source=chatgpt.com "Integrate with Prometheus | KEDA"
