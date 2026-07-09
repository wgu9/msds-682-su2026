# Demo 02: Producer Benchmark with Trip Events

**This demo produces local trip event messages into the same topic name created in Demo 01.**

Important: this demo is local-first. It does not send messages to Confluent Cloud by default.

Demo 01 created the real Confluent topic:

```text
msds682.demo01.trip-events.v1
```

Demo 02 uses that same topic name, but writes to a local JSONL file so everyone can run it without cloud credentials.

> Key classroom point: a producer creates messages and appends them to a topic. Demo 01 created an empty topic; Demo 02 shows what produced messages look like.

## Demo Goal

| Question | Answer |
|---|---|
| What are we producing? | Trip lifecycle event messages |
| Which topic name? | `msds682.demo01.trip-events.v1` |
| Does this hit Confluent Cloud? | No. This handout uses local JSONL transport. |
| Why local? | Students can learn producer behavior without API keys, cloud credits, or network issues. |
| What comes next? | Demo 03 reads the same topic name and replays by offset. |

## What You Should See at the End

Run:

```bash
python demo02_producer_benchmark.py --run-id lec2-demo02 --count 1000 --batch-size 100
```

Expected terminal output includes:

```json
{
  "topic": "msds682.demo01.trip-events.v1",
  "rows": [
    {
      "strategy": "sync_style",
      "message_count": 1000
    },
    {
      "strategy": "batched",
      "message_count": 1000
    }
  ]
}
```

The script also writes:

```text
outputs/runs/lec2-demo02/topics/msds682_demo01_trip-events_v1.jsonl
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark.csv
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark.png
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark_report.json
```

The `.jsonl` file is the local topic log. Each line is one produced message.

## One Message Looks Like This

```json
{
  "trip_id": "trip_981",
  "event_type": "driver_matched",
  "rider_id": "rider-981",
  "driver_id": "driver-003",
  "zone": "north",
  "event_time": "2026-07-04T10:01:00Z"
}
```

This is the message value. Kafka itself would store bytes; our local demo stores readable JSON so students can inspect it.

## Relation to Demo 01 and Demo 03

| Demo | What it does | Topic name |
|---|---|---|
| Demo 01 | Creates the empty Confluent topic | `msds682.demo01.trip-events.v1` |
| Demo 02 | Produces trip event messages locally | `msds682.demo01.trip-events.v1` |
| Demo 03 | Consumes/replays trip event messages locally | `msds682.demo01.trip-events.v1` |

The topic name is shared on purpose. The storage backend is different:

| Storage | Used by | Meaning |
|---|---|---|
| Confluent Kafka | Demo 01 topic creation | Real cloud topic exists |
| Local JSONL file | Demo 02 and Demo 03 | Local append-only log for reproducible class runs |

## High-Value Details to Notice

| Detail | Why it matters |
|---|---|
| `TOPIC_NAME = "msds682.demo01.trip-events.v1"` | Keeps topic creation, producer, and consumer demos aligned |
| `TripEvent` model | Defines the local message value shape for this producer |
| `event_type` | Shows that one topic can carry related lifecycle event types |
| `model_dump(exclude_none=True)` | Avoids writing fields like `driver_id: null` when the event does not have a driver yet |
| `produce(...)` | Appends one message at a time |
| `produce_many(...)` | Appends messages in batches |
| Benchmark CSV/PNG | Shows a simple way to compare producer strategies |
| JSONL file | Lets students open the topic log and inspect messages directly |
| Clean topic per strategy | Each strategy starts from an empty local topic so timing comparisons are isolated |

## Read the Core Code First

This is the important logic before you run anything.

```python
TOPIC_NAME = "msds682.demo01.trip-events.v1"


class TripEvent(BaseModel):
    # Local message value model for the ridesharing topic.
    # Kafka itself stores bytes; this model keeps our sample payloads consistent.
    trip_id: str
    event_type: Literal["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    rider_id: str
    event_time: str
    zone: str
    driver_id: str | None = None
    fare: float | None = Field(default=None, ge=0)


def make_trip_event(index: int, rng: random.Random) -> TripEvent:
    # Deterministic fake events: same seed + same count = same local topic log.
    event_types = ["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    event_type = event_types[index % len(event_types)]
    trip_number = 981 + (index // len(event_types))
    return TripEvent(
        trip_id=f"trip_{trip_number}",
        event_type=event_type,
        rider_id=f"rider-{trip_number}",
        driver_id=None if event_type == "trip_requested" else f"driver-{rng.randint(1, 8):03d}",
        fare=round(rng.uniform(10.0, 90.0), 2) if event_type == "trip_completed" else None,
        zone=["north", "south", "west"][index % 3],
        event_time=f"2026-07-04T10:{index % 60:02d}:00Z",
    )
```

```python
def produce(self, topic: str, payload: dict) -> None:
    path = self.topic_path(topic)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
```

The second block is the local version of producer behavior: append one serialized message to the topic log.

The benchmark runs two strategies. Each strategy starts from a clean local topic so the timing comparison is isolated. The final `.jsonl` file shows the messages from the last strategy run; the CSV and PNG are the comparison artifacts.

## What Each Piece Does

| Piece | What it does | Why students need it |
|---|---|---|
| `TripEvent` | Defines the local Python shape of message values | Makes producer output predictable |
| `make_trip_event(...)` | Generates deterministic sample trip events | Same seed means reproducible messages |
| `JsonlTransport` | Local append-only topic storage | Mimics Kafka behavior without cloud credentials |
| `produce(...)` | Writes one message | Easy mental model for producer basics |
| `produce_many(...)` | Writes a batch of messages | Introduces batching / throughput thinking |
| `producer_benchmark.csv` | Structured benchmark result | Easy to inspect or grade |
| `producer_benchmark.png` | Bar chart of messages/sec | Quick visual comparison |

## What This Demo Does Not Do

| Not included yet | Why |
|---|---|
| Create the topic in Confluent | Demo 01 already covers topic creation |
| Send messages to Confluent | This handout is local-first for reproducibility |
| Consume messages | Demo 03 covers offset/replay behavior |
| Enforce a Kafka schema | Pydantic validates locally; Schema Registry is a later/optional concept |
| Prove distributed Kafka performance | This is a local teaching benchmark, not a cloud load test |

## Step 1: Create a Working Folder

```bash
mkdir -p msds682-demos
cd msds682-demos
```

## Step 2: Create and Activate Python

Recommended:

```bash
uv python install 3.11
uv venv --python 3.11 .venv
source .venv/bin/activate
```

Fallback:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

## Step 3: Install Packages

```bash
python -m pip install --upgrade pip
python -m pip install pydantic pandas matplotlib
```

## Step 4: Download the Script

Download:

[demo02_producer_benchmark.py](handouts/demo02_producer_benchmark.py)

Put it in your working folder.

Your folder should look like:

```text
msds682-demos/
  demo02_producer_benchmark.py
  .venv/
```

No `.env` is required for this local demo.

## Step 5: Run Demo 02

```bash
python demo02_producer_benchmark.py --run-id lec2-demo02 --count 1000 --batch-size 100
```

For a shorter classroom check:

```bash
python demo02_producer_benchmark.py --run-id quick --count 20 --batch-size 5
```

## Step 6: Inspect the Local Topic Log

Open:

```text
outputs/runs/lec2-demo02/topics/msds682_demo01_trip-events_v1.jsonl
```

You should see one JSON object per line. Each line is one produced message.

Important: this local file is standing in for Kafka's append-only topic log. In real Kafka, messages are stored in partition logs on brokers, not in a plain `.jsonl` file.

## Step 7: Inspect the Benchmark Artifacts

Open:

```text
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark.csv
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark.png
```

The CSV records:

| Column | Meaning |
|---|---|
| `strategy` | Producer style being tested |
| `message_count` | Number of messages attempted |
| `batch_size` | Batch size used by the batched strategy |
| `elapsed_seconds` | Local wall-clock time |
| `throughput_msg_per_sec` | Messages per second in this local run |

Do not over-interpret the absolute speed. The point is to compare patterns, not to benchmark Confluent Cloud.

## Common Problems

| Error or symptom | Most likely cause | Fix |
|---|---|---|
| `ModuleNotFoundError: pydantic` | Packages not installed in the active environment | Activate `.venv`, then reinstall packages |
| No output files | Running from a different folder than expected | Check current directory with `pwd` |
| Chart file missing | `matplotlib` was not installed correctly | Reinstall `matplotlib` in the active environment |
| Topic file looks duplicated | You reused the same run folder and reran after editing | Use a new `--run-id` |
| Confused why Confluent metrics do not change | This demo is local JSONL, not cloud Kafka | Use Demo 01 only to check Confluent topic existence |

## Optional: How This Would Map to a Real Kafka Producer

The local demo writes:

```python
transport.produce(TOPIC_NAME, event.model_dump(exclude_none=True))
```

A real Confluent producer would use the same topic name, but send bytes over the network:

```python
producer.produce(
    topic="msds682.demo01.trip-events.v1",
    key=event.trip_id,
    value=json.dumps(event.model_dump(exclude_none=True)).encode("utf-8"),
)
producer.flush()
```

That cloud version is intentionally not the default here. First learn the message shape and producer flow locally; then connect it to Confluent.
