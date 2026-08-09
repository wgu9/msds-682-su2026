# Demo 11: Observable API for an Asynchronous Kafka Workflow

- **Lecture:** Lecture 11 - API Design for Streaming Systems
- **Python:** 3.11.14
- **FastAPI:** 0.139.0
- **Pydantic:** 2.13.4
- **Kafka client:** `confluent-kafka[avro,schemaregistry]==2.15.0`
- **Continuation:** Demo 05 command and event contract
- **Primary path:** fully local and credential-free
- **Optional path:** one bounded Confluent Cloud round trip through the existing Demo 05 topic

[Download `demo11-student.zip`](handouts/demo11-student.zip)

> ##### KEY CONCEPT
>
> `202 Accepted` proves that the system accepted an asynchronous request. It
> does not prove that business processing finished. A useful streaming API also
> gives the client one stable ID and a direct way to observe `pending`,
> `completed`, or `failed`.

> ##### CURRENT STATUS
>
> **Live-certified on August 9, 2026** with run ID
> `demo11-final-20260809`. The existing Demo 05 topic accepted one Avro event,
> one bounded Python consumer validated it, status changed from `pending` to
> `completed`, an identical retry returned `200`, a changed retry returned
> `409`, and `contract_passed` was `true`. No Confluent CLI, ksqlDB application,
> or Flink statement was required. Cloud state can change, so rerun Demo 11B
> before a future class instead of treating this dated proof as permanent.

## 1. Objective, expected outcome, and 16-minute route

Demo 11 answers one client-facing question:

> After an API accepts work for Kafka, how does the client know what happened?

By the end of Demo 11, you should be able to:

1. separate request acceptance from business completion;
2. trace one `request_id` across HTTP, Kafka, processing, and status lookup;
3. explain why an identical retry returns the existing logical request;
4. distinguish an event log from a queryable read model; and
5. keep the API contract stable when the compute owner changes.

<div class="handout-flow" role="group" aria-label="Demo 11 teaching loop">
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Accept</span>
    <strong>POST returns 202</strong>
    <p>FastAPI validates intent and publishes one governed event.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">→</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Process</span>
    <strong>Worker owns completion</strong>
    <p>Bounded processing changes the request from pending to completed.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">→</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Observe</span>
    <strong>GET returns status</strong>
    <p>The client reads current state without scanning the Kafka log.</p>
  </section>
</div>

| Step | Action | Main question | Time |
|---:|---|---|---:|
| 1 | Read the three contracts | What belongs at each boundary? | 3 minutes |
| 2 | POST, then GET | What does `202` prove? | 3 minutes |
| 3 | Complete one bounded worker step | Who owns business completion? | 5 minutes |
| 4 | Retry and conflict | How does one stable ID protect meaning? | 5 minutes |

Why this matters:

| Without an observable result contract | Demo 11 adds | Practical value |
|---|---|---|
| `202` is mistaken for success | Explicit `pending`, `completed`, and `failed` | Honest client behavior |
| A timeout creates duplicate work | Stable ID plus payload comparison | Safe retry |
| The API is tied to one worker | Fixed API around a replaceable compute owner | Easier system evolution |
| Operators cannot connect request to result | One ID across API, Kafka, logs, and status | Faster diagnosis |

## 2. Relationship to Demo 05 and Demo 09

Demo 05 implemented the command side and stopped at broker acknowledgement.
Demo 11 adds the result side. It does not create a second business story.

```text
Demo 05                                           Demo 11
HTTP request -> validation -> Kafka ack     ->   worker -> read model -> GET status
                    accepted                                   completed or failed
```

| Question | Demo 05 established | Demo 11 adds |
|---|---|---|
| What enters? | `CreateTripRequest` | The same request contract |
| What is retained? | `TripEventV1` in the Demo 05 topic | The same governed event |
| What does the first response mean? | Kafka accepted the event | Work remains `pending` |
| How does the caller observe progress? | Not covered | `GET /trip-requests/{request_id}` |
| What makes retry safe? | Stable request identity | Same payload returns `200`; changed payload returns `409` |

Demo 09 supplies one architecture lesson: Python, ksqlDB, or Flink SQL can own
downstream computation without changing the public API.

| Layer that stays fixed | Owner that may change |
|---|---|
| POST/GET contract, `request_id`, Kafka event, status vocabulary | Python worker, ksqlDB application, or Flink SQL job |

Demo 11 runs the Python owner live. It does not start ksqlDB or Flink.

## 3. Direct prerequisites and independence boundary

> **Start directly with the Demo 11 student ZIP.** It already includes every
> required Demo 05 contract and publisher module. Do not download or start Demo
> 05 separately.

| Requirement | Demo 11A local | Demo 11B Cloud |
|---|---:|---:|
| Python 3.11.14 and packaged requirements | Required | Required |
| Separate Demo 05 download or running process | No | No |
| Kafka cluster and Schema Registry | No | Required |
| Kafka and Registry credentials in `.env` | No | Required |
| Existing Demo 05 topic | No | Optional; use `--create-topic` if missing |
| Demo 09 SQL application or compute pool | No | No |

The package is **self-contained to run** but intentionally **extends the Demo
05 contract**. There is no Demo 11 Kafka topic and no duplicated event schema.

## 4. REST, FastAPI, and streaming-system API design

<div class="handout-flow" role="group" aria-label="Three API design layers">
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Protocol</span>
    <strong>REST / HTTP</strong>
    <p>Defines requests, responses, status codes, and resource URLs.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">+</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Implementation</span>
    <strong>FastAPI</strong>
    <p>Implements routes, validation, response models, and OpenAPI.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">+</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">System contract</span>
    <strong>Streaming API design</strong>
    <p>Defines durable work, completion, retry, failure, and observation.</p>
  </section>
</div>

FastAPI builds the front door. REST defines how clients use the door. Streaming
system design defines the durable workflow behind it.

## 5. One workflow, three contracts

| Boundary | Contract | Minimum responsibility |
|---|---|---|
| HTTP command | `CreateTripRequest` | Validate client intent |
| Kafka fact | `TripEventV1` | Retain the accepted business event |
| HTTP status | `TripStatusResponse` | Report current workflow state |

Similar fields do not make these the same model. Each contract has a different
owner, lifecycle, and reason to change.

```text
Client -> POST contract -> Kafka event -> processing -> status contract -> Client
          accepts intent   retains fact   owns work    answers what is known now
```

## 6. Status and retry contract

| Response | Meaning | Client action |
|---|---|---|
| `202 Accepted` | First valid request was accepted; work remains | Follow `status_url` |
| `200 OK` | Identical retry found the existing logical request | Continue using the same ID |
| `409 Conflict` | Same ID, different payload | Stop and correct the request |
| `404 Not Found` | No status exists for that ID | Check the ID or submission history |
| `422 Unprocessable Entity` | HTTP contract failed validation | Fix the named fields |
| `503 Service Unavailable` | Publisher could not accept work safely | Retry later with the same ID |

One rule controls retry behavior:

```text
same request_id + same payload       -> return existing request
same request_id + different payload  -> reject with 409
```

## 7. Files and ownership

| Existing owner reused from Demo 05 | New owner in Demo 11 |
|---|---|
| `demo05_common.py`: request, event mapping, topic name | `demo11_common.py`: status, fingerprint, SQLite repository |
| `demo05_app.py`: publisher interface and local publisher | `demo11_app.py`: idempotent POST and status GET |
| `demo05_kafka.py`: AIO publisher and bounded consumer | `demo11a_*` / `demo11b_*`: bounded teaching sequences |
| `trip_event_contract.py`: `TripEventV1` and Avro conversion | No second event or schema owner |

This split keeps one source of truth for the request, event, schema, and topic.

## 8. Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
```

Run the tests first. They require no Cloud credentials.

## 9. Demo 11A: local observable round trip

This is the primary classroom path:

```bash
python demo11a_local_observable_roundtrip.py \
  --run-id lec11-demo11a
```

<ol class="handout-pipeline-list" aria-label="Expected Demo 11A trace">
  <li><span>Accept</span><strong>POST returns 202, pending, and one stable status URL.</strong></li>
  <li><span>Observe</span><strong>GET returns pending before the worker completes.</strong></li>
  <li><span>Process</span><strong>One bounded worker step marks the request completed.</strong></li>
  <li><span>Observe again</span><strong>The same GET now returns completed.</strong></li>
  <li><span>Retry</span><strong>Identical POST returns 200 and creates no duplicate event.</strong></li>
  <li><span>Protect meaning</span><strong>Changed payload with the same ID returns 409.</strong></li>
</ol>

The final report should contain `contract_passed: true`. The local publisher is
a deterministic teaching double; it does not claim Kafka delivery guarantees.

## 10. Demo 11B: optional real Confluent round trip

Copy `.env.example` to ignored `.env`. Use the same credential types and topic
contract as Demo 05. Demo 11 uses the Python Confluent client, not the
Confluent CLI.

```bash
python demo11b_confluent_observable_roundtrip.py \
  --run-id lec11-demo11b
```

If the canonical Demo 05 topic is missing:

```bash
python demo11b_confluent_observable_roundtrip.py \
  --run-id lec11-demo11b \
  --create-topic
```

| Evidence | What it proves | What it does not prove |
|---|---|---|
| HTTP `202` plus broker receipt | Kafka acknowledged the event | Business processing completed |
| Validated bounded consume | One event reached the worker safely | A production worker runs forever |
| Status changes to `completed` | The client can observe this bounded result | Every future request will succeed |

## 11. Event log versus read model

| Kafka event log | SQLite read model |
|---|---|
| Full ordered history | Current status by `request_id` |
| Replay and independent processing | Fast point lookup for the API |
| Append-oriented | Query-oriented |
| Source for rebuilding state | Disposable local projection |

The GET route reads the projection; it does not scan Kafka for every request.
A production system may replace SQLite with another queryable store without
changing the API contract.

## 12. Common mistakes

| Symptom | Likely cause | Direct fix |
|---|---|---|
| Treating `202` as completion | Acceptance and processing were collapsed | GET the status resource |
| Duplicate logical work | Retry used a new ID or skipped fingerprint comparison | Reuse the original ID and payload |
| `409` on retry | Payload changed under the same ID | Use the original payload or a new ID |
| `404` from GET | Wrong or never-accepted ID | Read `status_url` from the POST response |
| `503` from Cloud POST | Kafka or Registry could not acknowledge safely | Check access, then retry with the same ID |
| Slow GET implementation | Route scans the Kafka log | Read a maintained projection |
| Missing topic | Canonical Demo 05 topic was deleted | Run Demo 11B once with `--create-topic` |

## 13. Review questions

1. What exactly does the first `202` prove?
2. Why are the request, event, and status separate contracts?
3. Why does an identical retry return the existing request?
4. Why does the GET route use a read model instead of scanning Kafka?
5. Which compute owner can change without changing the client contract?

## 14. Cleanup and safety

- Stop any local Python process after the bounded run.
- Demo 11 starts no Flink statement or ksqlDB application.
- Do not delete shared Demo 05 or Demo 07 topics during class cleanup.
- Never publish `.env`, API keys, secrets, or generated Cloud reports.
- Revoke obsolete credentials and stop unused paid Cloud compute separately.

## 15. Completion checklist

- [ ] Credential-free tests pass.
- [ ] The local run reports `contract_passed: true`.
- [ ] I can explain `202 -> pending -> completed`.
- [ ] An identical retry returns `200` without a second logical event.
- [ ] A changed payload with the same ID returns `409`.
- [ ] I can distinguish the Kafka event log from the status read model.
- [ ] I can identify which contracts come from Demo 05 and which owners Demo 11 adds.
- [ ] Any Cloud run is bounded and uses only the Python client.
- [ ] No `.env`, key, secret, or Cloud-generated report is submitted.

## 16. One-screen summary

| Design question | Demo 11 answer |
|---|---|
| What enters? | One validated HTTP command |
| What is durable? | One governed Kafka event |
| What does `202` mean? | Accepted, not completed |
| Who completes the work? | The downstream compute owner |
| How does the client observe it? | GET one status resource by `request_id` |
| What makes retry safe? | Same ID plus same-payload comparison |
| What can change internally? | Python, ksqlDB, or Flink SQL compute |
| What must stay stable? | The public API and business contracts |
