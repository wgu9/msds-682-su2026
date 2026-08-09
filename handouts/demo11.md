# Demo 11: Observable API for an Asynchronous Kafka Workflow

- **Lecture:** Lecture 11 - API Design for Streaming Systems
- **Continuation:** Demo 05
- **Primary path:** fully local and credential-free
- **Optional path:** one bounded Confluent Cloud round trip using the existing
  Demo 05 topic and Python client

[Download `demo11-student.zip`](handouts/demo11-student.zip)

## 1. What changes after Demo 05?

Demo 05 proved that a validated HTTP command could become a governed Kafka
event and return `202 Accepted` after broker acknowledgement. It intentionally
stopped before business completion.

Demo 11 completes the client contract:

```text
POST /trip-requests
        |
        v
Demo 05 publisher -> TripEventV1 -> bounded worker -> status repository
        |                                      |
        +----------- 202 + status URL          v
                                      GET /trip-requests/{request_id}
```

The new lesson is not more FastAPI syntax. It is how an API exposes the state
of asynchronous work without pretending that broker acknowledgement means
business completion.

## 2. One API contract, three compute owners

Lecture 9 and Demo 09 showed that Python, ksqlDB, and Flink SQL can own the same
stream-processing operation. The public API should not change when that
internal owner changes.

| Layer | Course owner | Responsibility |
|---|---|---|
| HTTP boundary | Demo 05 / Demo 11 | Validate intent and return an honest status |
| Durable event log | Kafka | Retain ordered events for independent processing |
| Compute | Python, ksqlDB, or Flink SQL | Own processing state, time, and recovery |
| Read model | Demo 11 status repository | Materialize current workflow state |
| Query boundary | Demo 11 | Return `pending`, `completed`, or `failed` |

Demo 11 runs the Python owner live. The API contract would remain the same if a
managed SQL job produced the downstream result.

## 3. REST, FastAPI, and streaming-system API design

| Layer | Question |
|---|---|
| REST / HTTP | How does the client send a request and receive a response? |
| FastAPI | How does Python implement routes, validation, status codes, and OpenAPI? |
| Streaming-system API design | What durable workflow starts behind the route, and how does the client observe completion? |

FastAPI builds the front door. REST defines how clients use the door.
Streaming-system API design defines the durable workflow behind it.

## 4. Status-code contract

| Response | Meaning |
|---|---|
| `202 Accepted` | First valid request was accepted; downstream work remains |
| `200 OK` | Identical retry found the existing logical request |
| `409 Conflict` | The same `request_id` was reused with a different payload |
| `404 Not Found` | No status row exists for that `request_id` |
| `422 Unprocessable Entity` | FastAPI rejected the HTTP contract |
| `503 Service Unavailable` | The event publisher could not accept the work safely |

## 5. Files and ownership

| File | Owns |
|---|---|
| `demo05_common.py` | Existing request model, event mapping, topic name, and deterministic input |
| `demo05_app.py` | Existing publisher interface and local publisher |
| `demo05_kafka.py` | Existing Confluent AIO publisher and bounded consumer |
| `demo11_common.py` | Status response, request fingerprint, SQLite repository, and bounded completion effect |
| `demo11_app.py` | POST idempotency and GET status routes |
| `demo11a_local_observable_roundtrip.py` | Credential-free classroom sequence |
| `demo11b_confluent_observable_roundtrip.py` | Optional bounded real-Kafka sequence |

There is no Demo 11 Kafka topic or duplicated event schema. Demo 11 reuses the
Demo 05 topic and `TripEventV1` contract.

## 6. Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Run the tests first:

```bash
python -m pytest -q
```

## 7. Demo 11A: local observable round trip

This is the primary classroom path. It needs no Kafka cluster, Registry, API
key, or secret.

```bash
python demo11a_local_observable_roundtrip.py \
  --run-id lec11-demo11a
```

Read the output in this order:

1. first POST returns `202`, `pending`, and a stable `status_url`;
2. first GET returns `pending`;
3. one bounded Python worker step marks the event `completed`;
4. the same GET returns `completed`;
5. identical POST retry returns `200` and publishes no second logical event;
6. changed payload with the same ID returns `409`;
7. `contract_passed` is `true`.

The local publisher is a deterministic teaching double. It preserves the API
contract but does not claim Kafka delivery guarantees.

## 8. Demo 11B: optional real Confluent round trip

Copy `.env.example` to `.env` and use the same credentials and topic contract
as Demo 05. Demo 11 uses the Python Confluent client; no Confluent CLI is
required.

```bash
python demo11b_confluent_observable_roundtrip.py \
  --run-id lec11-demo11b
```

If the existing Demo 05 topic has been removed, create that SSOT topic through
the same Python command:

```bash
python demo11b_confluent_observable_roundtrip.py \
  --run-id lec11-demo11b \
  --create-topic
```

The Cloud path proves:

```text
HTTP 202
  = broker acknowledgement
  != downstream completion

bounded consume + validated processing
  = status changes from pending to completed
```

## 9. Why use a read model?

Kafka owns retained events, not point-lookups for an HTTP client. The query
route reads a small SQLite projection instead of scanning a topic on every GET.

```text
event log                     read model
---------                     ----------
full ordered history   ->     current status by request_id
replay source                  fast API lookup
append-oriented                query-oriented
```

SQLite is the local teaching implementation. A production system might use a
database or another queryable store without changing the API contract.

## 10. Review questions

1. What exactly does the first `202` prove?
2. Why are `CreateTripRequest`, `TripEventV1`, and `TripStatusResponse`
   separate contracts?
3. Why does an identical retry return the existing request instead of
   publishing again?
4. Why is the Kafka log not queried directly by the GET route?
5. Which component could change from Python to ksqlDB or Flink SQL without
   changing the client contract?

## 11. Cleanup and safety

- Stop any local Python process after the bounded run.
- Demo 11 does not start a Flink statement or ksqlDB application.
- Do not delete shared Demo 05 or Demo 07 topics during class cleanup.
- Never publish `.env`, API keys, secrets, or generated Cloud reports.
- Revoke obsolete credentials and stop unused paid Cloud compute separately.

