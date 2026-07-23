# Demo 07: Real-time pricing, delayed outcomes, and model evaluation

- **Lecture:** Lecture 7, State, Features, and Real-time ML
- **Python:** 3.11.14
- **Kafka client:** `confluent-kafka[avro,schemaregistry]==2.15.0`
- **ML library:** `scikit-learn==1.9.0`
- **Validation:** `pydantic==2.13.4`
- **Routing:** open-source OSRM Route service or an explicit offline fixture
- **Cloud platform:** your Confluent Cloud environment

> ##### KEY CONCEPT
>
> A real-time prediction is not the end of an ML system. A later outcome
> provides the label needed to evaluate the prediction, compare model versions,
> and decide whether a candidate should be promoted.

## 1. Objective, expected outcome, and 55 to 70 minute route

Demo 07 answers one business question:

> How can we set a fare so that profit per trip is close to a 20% markup on
> realized cost?

The finished pipeline turns four trip requests into two comparable quote
versions, joins those quotes with delayed outcomes, and produces one
evidence-based teaching promotion decision.

By the end of Demo 07, you should be able to:

1. separate route features, cost prediction, and pricing policy;
2. explain why `rule-v1` is a baseline and `ridge-v2` is a trained model;
3. follow output acknowledgement before input offset commit;
4. join fare quotes with delayed outcomes by `trip_id`; and
5. compare model versions against one business metric.

The complete teaching loop is:

```text
07A  deterministic examples -> train ridge-v2 -> versioned JSON artifact

07B  trip request topic
             |
             v
07C  route features -> rule-v1 and ridge-v2 -> fare quote topic
                                                    |
07D  delayed trip outcome topic --------------------+
                                                    v
07E  bounded join by trip_id -> pricing evaluation topic
                                                    |
                                                    v
07F  compare versions -> teaching promotion decision
```

| Step | Demo | Question | Time |
|---:|---|---|---:|
| 1 | 07A | How is a model trained, versioned, and exported? | 8 minutes |
| 2 | 07B | What event starts the online pipeline? | 5 minutes |
| 3 | 07C | How do rule-v1 and ridge-v2 produce comparable quotes? | 15 minutes |
| 4 | 07D | What delayed label arrives after a trip completes? | 5 minutes |
| 5 | 07E | How are quotes joined with outcomes safely? | 15 minutes |
| 6 | 07F | Which version is closer to the business target? | 8 minutes |

## 2. Relationship to Demo 06

Demo 07 reuses the processing discipline established in Demo 06:

```text
consume -> validate -> compute or infer -> produce
        -> output ack -> commit input offset
```

Demo 06 uses one input topic to derive a stateless order metric and emphasizes
resume and replay. Demo 07 keeps that correctness boundary and adds two model
versions, delayed trip outcomes, a bounded join by `trip_id`, and business
evaluation. The conceptual sequence is intentional; the runtime resources are
independent.

| Demo 06 established | Demo 07 adds |
|---|---|
| Stateless input-to-output transformation | Route features and versioned pricing |
| Output acknowledgement before input commit | The same correctness boundary for quotes and evaluations |
| Consumer-group resume and replay | Two model-version groups receive the same requests |
| Kafka offsets record processing progress | Join state pairs quotes with delayed outcomes |

## 3. Direct prerequisites

> **Start directly with Demo 07.** It creates its own data, topics, schemas,
> consumer groups, offsets, and model artifact. No Demo 01–06 resource or
> running process is required.

| Requirement | 07A and 07F | 07B–07E |
|---|---:|---:|
| Python 3.11.14 and published requirements | Required | Required |
| Confluent Cloud Kafka cluster | No | Required |
| Schema Registry enabled for Avro | No | Required |
| Kafka and Registry credentials in `.env` | No | Required |
| Existing Demo 07 topics or records | No | No; 07B creates and seeds them |
| Outbound HTTPS | No | Only 07C and 07D in OSRM mode |
| Demo 06 topic, data, or process | No | No |

07A is standalone. 07F is credential-free but not standalone: by default it
reads `outputs/runs/<run-id>/demo07e/report.json`. Run 07B–07E first, or pass
`--evaluation-report` explicitly.

## 4. Download and setup

- [Download `demo07-student.zip`](handouts/demo07-student.zip)

Extract it, enter its one top-level folder, and run:

```bash
uv venv --python 3.11.14 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
pytest -q
```

Then copy `.env.example` to `.env` and fill the Kafka and Schema Registry
credentials. `.env` is ignored and must never be published or submitted.

## 5. Data and scale

All inputs are synthetic. Training and validation examples, trip requests, and
fixture routes are deterministic. Live OSRM route measurements may change with
the public service's map data.

| Data | Count | Purpose |
|---|---:|---|
| Training examples | 120 | Fit the Ridge cost model |
| Validation examples | 40 | Measure held-out cost prediction error |
| Online trip requests | 4 | Keep the live classroom run small |
| Quote versions per trip | 2 | Compare rule-v1 and ridge-v2 fairly |
| Delayed outcomes | 4 | Provide realized cost labels from the same route mode |
| Evaluation events | 8 | One result per trip and model version |

Each `TripRequestV1` uses these fields:

| Contract field | Meaning |
|---|---|
| `requested_at` | Request date and time |
| `pickup` | Pickup latitude and longitude |
| `dropoff` | Destination latitude and longitude |

OSRM receives the pickup and dropoff and returns estimated distance and
duration in one response.

The first deterministic fixture is:

```text
estimated distance = 2.00 miles
estimated duration = 15.0 minutes
```

$$
\text{rule-v1 fare}
= 3.50 \times 2.00 + 0.20 \times 15.0
= 10.00\text{ USD}
$$

## 6. The business target

The course uses **20% markup on cost**:

$$
\text{realized markup}
= \frac{\text{fare} - \text{actual cost}}{\text{actual cost}}
$$

$$
\text{target fare}
= 1.20 \times \text{predicted cost}
$$

This is not the same as a 20% profit margin:

$$
\text{profit margin}
= \frac{\text{fare} - \text{cost}}{\text{fare}},
\qquad
\text{fare}_{20\%\text{ margin}}
= \frac{\text{cost}}{1 - 0.20}
= 1.25 \times \text{cost}
$$

Demo 07 consistently uses markup because the business question defines profit
relative to cost.

### Rule v1

The transparent baseline is the original heuristic:

$$
F_{\text{rule-v1}}
= 3.50 \cdot D + 0.20 \cdot T
$$

Here, `F_rule-v1` is the estimated fare in USD, `D` is estimated distance in
miles, and `T` is estimated duration in minutes.

It is versioned and reproducible, but it is not ML and does not explicitly
target 20% markup.

### Ridge v2

The candidate first predicts trip cost:

$$
\widehat{C}
= \beta_0 + a \cdot D + b \cdot T,
\qquad
F_{\text{ridge-v2}}
= 1.20 \times \widehat{C}
$$

Here, `C-hat` is predicted trip cost and `F_ridge-v2` is the estimated fare
after applying the 20% markup policy. `Ridge` is fitted from historical
synthetic examples. The artifact records the feature names, coefficients,
intercept, training seed, library version, and validation MAE.

### Optional advanced discussion: why Ridge?

This discussion is not required to run Demo 07. The model is deliberately
small so the course can focus on the end-to-end streaming ML lifecycle.

Distance and duration are usually correlated, but they are not identical. A
short congested route can take longer than a longer freeway route. That
variation helps the model distinguish distance-related cost from time-related
cost. If duration were almost a fixed multiple of distance, the model could
estimate their combined effect but could not reliably identify how much cost
belongs to each feature.

| Method | Behavior with correlated features | Demo 07 interpretation |
|---|---|---|
| Ordinary linear regression | Coefficients can change substantially across similar samples | Simple and valid, but potentially less stable |
| Lasso regression | May force one correlated coefficient to exactly zero | Not preferred because both distance and time are intended cost drivers |
| Ridge regression | Shrinks large coefficients and usually keeps both features | A stable, explainable teaching choice |

Ridge minimizes prediction error plus an L2 penalty:

$$
\text{Ridge loss}
= \sum_i\left(C_i-\widehat{C}_i\right)^2
+ \lambda\left(a^2+b^2\right)
$$

The penalty stabilizes coefficient selection; it does not prove that the
coefficients are causal, and it does not guarantee that they are positive.
Demo 07 fixes the scikit-learn regularization strength at `alpha=1.0` so every
student runs the same deterministic comparison. It does not tune a large
hyperparameter search or claim that Ridge is the best production pricing
model.

In a production model, engineers might scale features, tune the regularization
strength, add nonnegative coefficient constraints, or keep known accounting
costs as explicit rules and train ML only on the remaining cost. Those choices
are intentionally outside this demo.

> ##### IMPORTANT NOTE
>
> The synthetic realized cost is:
>
> $$
> C_{\text{actual}}
> = 5 + 0.75 \cdot D_{\text{actual}}
> + 20 \times \left(\frac{T_{\text{actual}}}{60}\right)
> + \varepsilon
> $$
>
> Here, `epsilon` is small deterministic noise generated from the fixed course
> seed.
>
> The distance component is intentional. If the label used only fixed cost and
> hourly time, the correct learned distance coefficient would be approximately
> zero.

## 7. Topic and consumer design

`demo07_common.py` is the SSOT for all topic names and business constants.

| Owner | Topic | Key | Value contract |
|---|---|---|---|
| Request source | `msds682.demo07.ml-trip-requests-avro.v1` | `trip_id` | `TripRequestV1` |
| Quote processor | `msds682.demo07.ml-fare-quotes-avro.v1` | `trip_id` | `FareQuoteV1` |
| Outcome source | `msds682.demo07.ml-trip-outcomes-avro.v1` | `trip_id` | `TripOutcomeV1` |
| Evaluator | `msds682.demo07.ml-pricing-evaluations-avro.v1` | `quote_id` | `PricingEvaluationV1` |

Classroom configuration:

- one partition per topic for a clear ordered demonstration;
- Avro values with TopicNameStrategy subjects;
- manual Kafka offset commits in both processors;
- one request consumer group per model version; and
- one evaluator group consuming quote and outcome topics.

Because rule-v1 and ridge-v2 use different consumer groups, both receive every
request and produce one quote per trip. After evaluation, a production system
would normally deploy only the promoted version, so each new request would
produce one official fare quote.

## 8. Why Architecture A is implemented

### A: one routing and pricing processor

```text
trip request -> one OSRM response with miles and minutes -> fare quote
```

- 2 topics on the online quote path: requests and quotes;
- 1 active pricing processor after model promotion;
- 1 routing call per trip in that steady-state design;
- no online join state.

The classroom comparison deliberately runs rule-v1 and ridge-v2 as separate
processors, so each request is routed once per version. Demo 07D makes one
additional routing call per trip to create the synthetic delayed outcome.

### B: split mileage and duration

```text
trip request -> mileage topic  --+
             -> duration topic --+-> join -> fare quote
```

- 4 topics on the online quote path: requests, mileage, duration, and quotes;
- 3 processors;
- potentially 2 routing calls per trip;
- join state, timeout, duplicate, and late-event policy.

B is technically possible, but it creates an artificial join because one OSRM
response already owns both fields. It is kept as a design comparison, not as
the runnable baseline.

The meaningful stateful join is:

```text
fare quote + delayed trip outcome -> business evaluation
```

No ksqlDB, Kafka Streams, Flink, or Spark is required. The runnable baseline
uses Python `confluent-kafka`. The finite evaluator uses in-memory dictionaries
only to make state visible.

## 9. Run Demo 07

Choose one unique run ID and one routing mode, then reuse both:

```bash
RUN_ID=lec7-demo07-yourname
ROUTING_MODE=osrm
```

Use `fixture` instead of `osrm` for a deterministic offline route. 07C and 07D
must use the same mode; mixing them creates invalid label lineage. The code
never silently falls back between providers. The public OSRM endpoint is
used only for this small bounded teaching run: 12 calls total for four trips,
two quote versions, and four outcomes. It has no production service guarantee.

### 07A: train ridge-v2

**Objective:** Train and export one transparent, versioned cost model.

**Why:** Online inference needs a validated artifact whose inputs, coefficients,
training lineage, and held-out error can be inspected.

**Done when:** `ridge-cost-v2.json` and a secret-free report exist, and the
artifact contains the two named route features plus validation MAE.

```bash
python demo07a_train_cost_model.py --run-id "$RUN_ID"
```

Expected terminal lines:

```text
Trained ridge-v2 on 120 records; validation MAE=... cents
Model artifact: outputs/runs/$RUN_ID/demo07a/ridge-cost-v2.json
Secret-free report: outputs/runs/$RUN_ID/demo07a/report.json
```

The artifact is validated JSON, not an executable pickle.

### 07B: create topics and publish requests

**Objective:** Create the four Demo 07 topics and publish the same four
deterministic trip requests used by both pricing versions.

**Why:** A fair model comparison begins with identical input events and visible
broker acknowledgements.

**Done when:** All four requests are acknowledged and all four topics are
verified.

```bash
python demo07b_produce_trip_requests.py \
  --run-id "$RUN_ID" \
  --count 4 \
  --create-topics
```

Expected terminal lines:

```text
Published 4 requests to msds682.demo07.ml-trip-requests-avro.v1
Secret-free report: outputs/runs/$RUN_ID/demo07b/report.json
```

### 07C: publish two comparable quote versions

**Objective:** Consume every request once per model version and publish one
governed fare quote per version.

**Why:** Separate consumer groups let the baseline and candidate see the same
requests while preserving the acknowledgement-before-commit boundary.

**Done when:** `rule-v1` and `ridge-v2` each acknowledge four quotes, for eight
quotes total, and each processor commits only after its output acknowledgement.

#### Run 1: rule-v1 baseline

```bash
python demo07c_confluent_fare_quote_processor.py \
  --run-id "$RUN_ID" \
  --pricing-method rule-v1 \
  --max-messages 4 \
  --routing-mode "$ROUTING_MODE"
```

Expected terminal lines:

```text
rule-v1 published 4 quotes to msds682.demo07.ml-fare-quotes-avro.v1
Secret-free report: outputs/runs/$RUN_ID/demo07c-rule-v1/report.json
```

#### Run 2: ridge-v2 candidate

```bash
python demo07c_confluent_fare_quote_processor.py \
  --run-id "$RUN_ID" \
  --pricing-method ridge-v2 \
  --model-artifact "outputs/runs/$RUN_ID/demo07a/ridge-cost-v2.json" \
  --max-messages 4 \
  --routing-mode "$ROUTING_MODE"
```

Expected terminal lines:

```text
ridge-v2 published 4 quotes to msds682.demo07.ml-fare-quotes-avro.v1
Secret-free report: outputs/runs/$RUN_ID/demo07c-ridge-v2/report.json
```

Each quote processor follows:

```text
consume request
-> validate Avro and Pydantic contract
-> call routing provider once
-> execute pricing version
-> produce quote
-> wait for broker acknowledgement
-> commit Kafka input offset
```

Here, `commit` means a **consumer offset commit**. It does not mean a Git
commit, and a Kafka producer does not commit these input offsets.

### 07D: publish delayed outcomes

**Objective:** Publish the realized trip outcomes that arrive after pricing.

**Why:** A quote is a prediction. Only a later outcome supplies the label needed
to measure realized markup.

**Done when:** Four outcomes are acknowledged for the same four `trip_id`
values and the same routing mode used by 07C.

Run this after both quote versions:

```bash
python demo07d_produce_trip_outcomes.py \
  --run-id "$RUN_ID" \
  --count 4 \
  --routing-mode "$ROUTING_MODE"
```

Expected terminal lines:

```text
Published 4 delayed outcomes to msds682.demo07.ml-trip-outcomes-avro.v1
Secret-free report: outputs/runs/$RUN_ID/demo07d/report.json
```

### 07E: join and evaluate

**Objective:** Join quotes with delayed outcomes by `trip_id` and publish one
evaluation event per trip and model version.

**Why:** Subscribing to two topics does not perform a join. The evaluator must
hold bounded keyed state until both sides arrive.

**Done when:** Eight pairs produce eight acknowledged evaluations, then the
evaluator commits progress for both input topics.

```bash
python demo07e_confluent_quote_outcome_evaluator.py \
  --run-id "$RUN_ID" \
  --expected-trips 4
```

Expected terminal lines:

```text
Published 8 evaluations to msds682.demo07.ml-pricing-evaluations-avro.v1
Model summary: {...}
Secret-free report: outputs/runs/$RUN_ID/demo07e/report.json
```

### 07F: compare and decide

**Objective:** Compare both versions against the 20% realized-markup target and
make a reproducible teaching promotion decision.

**Why:** A model version should be selected from shared outcome evidence and a
declared business metric, not from its name or training status.

The primary comparison metric is:

$$
\operatorname{MAE}_{\text{markup}}
= \frac{1}{N}\sum_{i=1}^{N}
\left|\text{realized markup}_{i} - 20\%\right|
$$

The report expresses this error in percentage points.

**Done when:** The comparison uses the same four outcomes, reports both errors,
and selects `ridge-v2` under the published promotion rule.

```bash
python demo07f_compare_models.py --run-id "$RUN_ID"
```

Expected terminal lines:

```text
Winner: ridge-v2 | baseline error=... pp | candidate error=... pp
Decision: promote ridge-v2
Secret-free report: outputs/runs/$RUN_ID/demo07f/report.json
```

## 10. Classroom boundary and production gaps

This is a complete **teaching loop**, not a production-complete ML platform.
In production, the quote processor would poll continuously and each new request
would produce a quote. The classroom scripts stop after fixed counts so every
record, offset, and output remains inspectable.

| Layer | Coverage |
|---|---|
| Event source and ingestion | Complete for the demo |
| Kafka event log | Complete |
| Contract governance | Complete |
| Stateless route feature transformation | Complete |
| Stateful stream processing | Partial: bounded in-memory quote-outcome join |
| Online model inference | Complete for one small trained model |
| Prediction event | Complete |
| Outcome and delayed labels | Complete |
| Monitoring and drift | Partial: finite metrics, no continuous monitor |
| Evaluation and retraining | Partial: initial training and evaluation, no retraining loop |
| Model versioning and promotion | Partial: artifact plus manual teaching decision |

Not covered: durable join state, event-time and late-event policy, continuous
monitoring, model registry and rollback, automatic retraining, or a production
routing service.

## 11. Evidence and common mistakes

Secret-free reports are written under:

```text
outputs/runs/$RUN_ID/
├── demo07a/report.json
├── demo07a/ridge-cost-v2.json
├── demo07b/report.json
├── demo07c-rule-v1/report.json
├── demo07c-ridge-v2/report.json
├── demo07d/report.json
├── demo07e/report.json
└── demo07f/report.json
```

Common mistakes:

- calling 20% markup a 20% profit margin;
- calling rule-v1 an ML model;
- committing an input before its output is acknowledged;
- assuming subscribing to two topics automatically performs a join;
- treating the bounded in-memory join as production durable state; or
- publishing `.env`, API keys, or raw credentials.

## 12. Completion checklist

- [ ] Credential-free tests pass.
- [ ] Ridge artifact has two named features and a validation MAE.
- [ ] Four request events are acknowledged.
- [ ] rule-v1 and ridge-v2 each publish four quotes.
- [ ] Four delayed outcomes are acknowledged.
- [ ] The evaluator joins by `trip_id`, publishes eight evaluations, and then commits.
- [ ] The comparison reports realized markup and model-version evidence.
- [ ] You can explain the A/B decision and name the remaining production gaps.

## 13. Cleanup

After class:

1. stop every Python process;
2. inspect the four Demo 07 topics and four Schema Registry subjects;
3. save only secret-free reports you need;
4. delete the Demo 07 cloud resources if they are no longer needed; and
5. continue monitoring Confluent Cloud cost and credits.
