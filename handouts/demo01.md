# Demo 01: Create a Kafka Topic with Python

This demo creates a real Kafka topic in Confluent Cloud. A producer needs a topic before it can send events, so this is the first cloud-side Kafka workflow in the course.

> Key classroom point: get Kafka API keys from Confluent, keep them in `.env`, load them with `python-dotenv`, then use `AdminClient` to create the topic.

## What You Should See at the End

Run:

```bash
python demo01_create_topic.py --run-id lec2
```

Expected terminal output:

```json
{
  "status": "created",
  "topic": "msds682.demo01.trip-events.v1",
  "partitions": 3,
  "replication_factor": 3,
  "cleanup_policy": "delete",
  "bootstrap_host": "YOUR_CLUSTER_HOST:9092",
  "has_username": true,
  "has_password": true
}
```

If you run it again, `status` should become `already_exists`. That is correct. The script is safe to rerun.

The script also writes:

```text
outputs/runs/lec2/demo01_topic_creation/topic_report.json
```

## The Example Topic

We will create this ridesharing event topic:

```text
msds682.demo01.trip-events.v1
```

Think of the topic as an event history for trips:

| Topic concept | In this demo |
|---|---|
| Topic name | `msds682.demo01.trip-events.v1` |
| Event examples | `trip_requested`, `driver_matched`, `trip_started`, `trip_completed` |
| Key idea | Later producers will send trip events into this topic |
| Why versioned | `v1` leaves room for a future schema/topic change |

## Read the Core Code First

This is the important logic before you run anything.

```python
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
import os


def load_config():
    load_dotenv()
    return {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": os.getenv("SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS", "PLAIN"),
        "sasl.username": os.getenv("SASL_USERNAME"),
        "sasl.password": os.getenv("SASL_PASSWORD"),
    }


def topic_exists(admin_client, topic_name):
    return topic_name in admin_client.list_topics(timeout=10).topics


def create_topic(admin_client, topic_name):
    if topic_exists(admin_client, topic_name):
        return "already_exists"

    topic = NewTopic(
        topic_name,
        num_partitions=3,
        replication_factor=3,
        config={"cleanup.policy": "delete"},
    )
    futures = admin_client.create_topics([topic])
    futures[topic_name].result(timeout=30)
    return "created"


conf = load_config()
admin_client = AdminClient(conf)
status = create_topic(admin_client, "msds682.demo01.trip-events.v1")
print(status)
```

Download the complete runnable script:

[demo01_create_topic.py](handouts/demo01_create_topic.py)

## What Each Piece Does

| Piece | What it does | Why students need it |
|---|---|---|
| Confluent API key | Username/secret pair that lets Python connect to your Kafka cluster | Without it, the Python client cannot authenticate |
| `.env` | Local file that stores secrets outside source code | Keeps credentials out of GitHub, Canvas, screenshots, and AI tools |
| `python-dotenv` | Reads `.env` and loads values into `os.getenv(...)` | Lets code use secrets without hardcoding them |
| `AdminClient` | Kafka admin client from `confluent-kafka` | Creates topics, checks metadata, and manages Kafka resources |
| `NewTopic` | Topic definition object | Specifies topic name, partitions, replication factor, and config |
| `topic_exists` | Metadata check | Makes the script idempotent, so rerunning does not fail |
| JSON report | Secret-free run evidence | TA can inspect result without seeing your API secret |

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
python -m pip install confluent-kafka python-dotenv
```

## Step 4: Get Kafka API Keys from Confluent

In Confluent Cloud:

1. Open your environment and Kafka cluster.
2. Go to API keys.
3. Create a Kafka cluster API key.
4. Copy the API key and API secret immediately.
5. Copy your cluster bootstrap server, usually ending in `:9092`.

> Use a Kafka cluster API key, not your Confluent website password.

## Step 5: Create `.env`

Create a file named `.env` in the same folder as `demo01_create_topic.py`.

```text
BOOTSTRAP_SERVERS=YOUR_CLUSTER_HOST:9092
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISMS=PLAIN
SASL_USERNAME=YOUR_KAFKA_API_KEY
SASL_PASSWORD=YOUR_KAFKA_API_SECRET
DEMO01_TOPIC_NAME=msds682.demo01.trip-events.v1
```

Example shape, with fake values:

```text
BOOTSTRAP_SERVERS=pkc-abc12.us-west2.gcp.confluent.cloud:9092
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISMS=PLAIN
SASL_USERNAME=ABCD1234EFGH5678
SASL_PASSWORD=fakeSecretDoNotUse_fakeSecretDoNotUse_fakeSecretDoNotUse
DEMO01_TOPIC_NAME=msds682.demo01.trip-events.v1
```

Never submit or screenshot real `.env` values.

## Step 6: Download the Script

Download:

[demo01_create_topic.py](handouts/demo01_create_topic.py)

Put it in the same folder as `.env`.

Your folder should look like this:

```text
msds682-demos/
  .env
  demo01_create_topic.py
  .venv/
```

## Step 7: Run Demo 01

```bash
python demo01_create_topic.py --run-id lec2
```

If successful, you should see JSON with:

- `status`: `created` or `already_exists`
- `topic`: `msds682.demo01.trip-events.v1`
- `partitions`: `3`
- `replication_factor`: `3`
- `has_username`: `true`
- `has_password`: `true`

The report deliberately does not print your API key or secret.

## Optional: Use Your Own Topic Name

If many students share one cluster, add your USF username or initials:

```bash
python demo01_create_topic.py \
  --topic msds682.demo01.trip-events.yourname.v1 \
  --run-id lec2
```

## Common Problems

| Error or symptom | Most likely cause | Fix |
|---|---|---|
| `Missing required .env values` | `.env` is missing or in the wrong folder | Put `.env` next to the script or run from the folder containing `.env` |
| Authentication failure | Wrong API key/secret or copied website password | Create a Kafka cluster API key in Confluent |
| Timeout | Bootstrap server is wrong or network is blocked | Recopy the cluster bootstrap server |
| Topic already exists | You already created it | This is fine; continue |
| Secret appears in screenshot | `.env` or hardcoded config is visible | Do not share the screenshot; rotate the API key if exposed |

## Why We Do Not Hardcode Secrets

Older classroom examples sometimes showed credentials directly in code for teaching speed. In this course, use `.env` instead:

```python
# Do not do this:
"sasl.password": "real-secret-in-source-code"

# Do this:
"sasl.password": os.getenv("SASL_PASSWORD")
```

The engineering habit matters more than this small demo: production code should not hardcode secrets.
