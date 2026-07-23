# Demo 06: Kafka Connect and bounded stream processing

- **Lecture:** Lecture 6 - Kafka Connect and Stream Processing
- **Python:** 3.11.14
- **Kafka client:** `confluent-kafka[avro,schemaregistry]==2.15.0`
- **Validation:** `pydantic==2.13.4`
- **Cloud platform:** your Confluent Cloud environment
- **Input topic:** `msds682.demo06.connector-orders-avro.v1`
- **Derived topic:** `msds682.demo06.connector-order-metrics-avro.v1`

> ##### KEY CONCEPT
>
> Kafka Connect moves records between systems. Kafka retains records. A stream
> processor validates input, computes a new fact, publishes a derived event,
> and records its consumer progress.

## 1. Objective, expected outcome, and 40-50-minute route

### What you will build

Build and verify one self-contained Confluent pipeline:

```text
managed source
  -> Kafka Connect
  -> Avro input records in Kafka
  -> bounded Python processor
  -> Avro derived records in Kafka
  -> output acknowledgement
  -> input consumer offset commit
  -> verified resume and replay
```

The final objective is not merely to run four scripts. It is to prove that data
can enter Kafka through a managed integration, become a validated derived fact,
and be processed with an explainable recovery boundary.

By the end of Demo 06, you should be able to:

1. distinguish Kafka Connect, Kafka, and a stream processor;
2. explain source connector, worker, connector, task, and converter;
3. inspect schema-aware records written by a managed source connector;
4. follow `consume -> validate -> derive -> produce -> output ack -> commit`;
5. prove same-group resume and explicit new-group replay; and
6. explain why this baseline is at-least-once, not an automatic exactly-once
   guarantee.

| Step | Demo | Main question | Time |
|---:|---|---|---:|
| 1 | 06A: source integration | How does data enter Kafka without a custom producer application? | 10-12 minutes |
| 2 | 06B: source inspection | Did Kafka receive valid Avro records from that source? | 8-10 minutes |
| 3 | 06C: processor | When is it safe to commit an input offset? | 12-15 minutes |
| 4 | 06D: resume and replay | What changes when the group ID changes? | 8-10 minutes |

**Start here:**

- Standard route: Section 4 -> Section 5 -> Section 7 -> Section 8 ->
  Section 9 -> Section 12 -> Section 13.
- No managed connector permission: Section 4 -> Section 6 -> Section 7 ->
  Section 8 -> Section 9 -> Section 12 -> Section 13.

The whole route, including the no-permission fallback branch:

```text
   06A managed Datagen connector          fallback seed (only if the
   ORDERS / AVRO / orderid / 2000 ms      account cannot create a
              |                           managed connector; run once)
              +------------+------------------------+
                           v
        input topic  msds682.demo06.connector-orders-avro.v1
                           |
                           v
   06B inspect   read 3 records, validate, commit nothing
                           |
                           v
   06C process   consume -> validate -> derive -> produce
                 -> output ack -> commit input offset
                           |
                           v
   06D prove     resume: same group continues after commits
                 replay: new group forced to OFFSET_BEGINNING
```

## 2. Relationship to Demo 05

Demo 05 and Demo 06 are connected conceptually, but Demo 06 is self-contained.
No Demo 05 topic, data, offsets, server, or process is required.

| Demo 05 established | Demo 06 adds |
|---|---|
| FastAPI receives an HTTP command | Kafka Connect supplies another data-entry path |
| Pydantic validates application meaning | Pydantic validates connector-created values |
| Avro and Schema Registry govern Kafka values | A processor writes a second governed contract |
| A bounded consumer processes before commit | Output acknowledgement occurs before input commit |
| One API request becomes one durable event | One durable input event becomes one derived event |

The system boundary is:

```text
External source
  -> Kafka Connect source connector
  -> input topic
  -> Python consumer processor
  -> Pydantic validation
  -> derived event
  -> output topic
  -> output broker acknowledgement
  -> input consumer offset commit
```

`commit` in this demo always means a **Kafka consumer offset commit**. It is
not a producer acknowledgement and it is unrelated to a Git commit.

### Bridge to Demo 07

Demo 06 is the conceptual foundation for Demo 07, but it is not a runtime
prerequisite. Both use the same correctness spine:

```text
consume -> validate -> compute -> produce -> output ack -> commit input offset
```

Demo 06 applies that spine to a stateless order-metric transformation and then
proves resume and replay. Demo 07 applies it to versioned fare estimation, then
adds delayed outcomes, a bounded stateful join, and model evaluation. Demo 07
creates its own topics, schemas, data, groups, and offsets, so no Demo 06 cloud
resource is reused.

## 3. Direct prerequisites

> **Start directly with Demo 06.** No Demo 01-05 resource is required.

| Requirement | 06A | 06B | 06C | 06D |
|---|---:|---:|---:|---:|
| Python 3.11.14 and the published requirements | Required | Required | Required | Required |
| Confluent Cloud Kafka cluster | Required | Required | Required | Required |
| Schema Registry enabled for Avro | Required | Required | Required | Required |
| Local Schema Registry credentials in `.env` | No | Required | Required | Required |
| Managed connector permission | Preferred | No | No | No |
| Existing Demo 06 topics | No; create them | Required | Required | Required |
| Existing source records | No | At least 3 | At least 3 | At least 6 |
| Demo 05 app or producer running | No | No | No | No |

If your classroom account cannot create a managed connector, use the explicit
finite fallback in Section 6. Do not run both input sources for the same
exercise. One input topic and its Schema Registry value subject must have one
schema owner for the exercise.

## 4. Download and setup

- [Download `demo06-student.zip`](handouts/demo06-student.zip)

Extract it, enter its one top-level folder, and run:

```bash
uv venv --python 3.11.14 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
pytest -q
```

Copy `.env.example` to `.env` and fill the Kafka and Schema Registry
credentials. `.env` is ignored and must never be submitted.

## 5. Demo 06A: managed Datagen Source connector

**Objective:** Establish the source-integration boundary: a managed connector,
not a custom Python producer, creates schema-aware input records in Kafka.

**Why:** Production data often begins in databases, object stores, or external
services. Kafka Connect separates that integration work from application
processing logic and lets the converter govern the Kafka wire format.

**Done when:** The connector and task are `RUNNING`, the input topic contains at
least 8 connector-created `ORDERS` records, and its Avro value subject is
registered in Schema Registry. Pause the connector before continuing.

First create and verify the two one-partition topics:

```bash
python demo06a_connect_source_plan.py \
  --run-id lec6-demo06a-yourname \
  --create-topics
```

Then open Confluent Cloud:

1. Select your current Kafka cluster.
2. Open **Connectors** and choose **Datagen Source**.
3. Use the fields written in the 06A report:
   - output topic: `msds682.demo06.connector-orders-avro.v1`;
   - output data format: `AVRO`;
   - quickstart: `ORDERS`;
   - schema key field: `orderid`;
   - tasks: `1`;
   - maximum interval: `2000 ms`.
4. Let Confluent Cloud manage or create the connector credentials.
5. Wait until the connector and its task report `RUNNING`.
6. Confirm at least 8 records in the input topic.
7. **Pause the connector before continuing.** Delete it after the exercise.

The advanced configuration should look like this. No API key or secret is
visible in the screenshot:

![Demo 06A Datagen advanced configuration](assets/demo06/demo06a-connector-configuration.jpg)

When provisioning succeeds, the connector shows `RUNNING`, the `ORDERS` data
set, and the dedicated input topic:

![Demo 06A managed Datagen connector running](assets/demo06/demo06a-connector-running.jpg)

> ##### IMPORTANT NOTE
>
> Datagen is a synthetic development/testing connector. It is useful for this
> exercise but is not a production source. A production source connector would
> read from an external database, object store, or service.

### What 06A proves

- the integration runtime can create source records without a custom Python
  producer;
- the connector owns one or more tasks;
- the Avro converter registers the value schema in Schema Registry;
- connector/task status is operational evidence.

It does not prove that your Python processing logic is correct. That begins in
06B and 06C.

## 6. Connector-permission fallback

Use this only when managed connector creation is unavailable:

```bash
python demo06_seed_source.py \
  --run-id lec6-demo06-seed-yourname \
  --count 8 \
  --create-topics
```

The fallback writes finite deterministic values with the managed Datagen
`ORDERS` value schema. Its report identifies itself as a Python fallback. It
does not claim that Kafka Connect ran.

The two source modes share the value contract, but not necessarily the raw key
encoding. The managed connector encodes its configured `orderid` key; the
fallback uses readable UTF-8 decimal bytes. Do not mix both source modes in one
exercise or infer the value schema from key length.

Use a fresh Demo 06 topic. Do not first register a different schema under the
same `<topic>-value` subject and then point Datagen at that topic. Do not weaken
Schema Registry compatibility to work around a subject ownership conflict.

## 7. Demo 06B: inspect connector-created source records

**Objective:** Verify the source boundary before allowing a processor to claim
progress: deserialize, validate, and record evidence for three input records
without committing offsets.

**Why:** A running connector proves only that integration infrastructure is
active. It does not prove that the application can decode and understand the
records. The inspection group must also remain separate from the processor's
committed progress.

**Done when:** Three Avro values pass `DatagenOrderV1` validation, the report
contains their topic-partition-offset coordinates, and `manual_commits` is
zero.

```bash
python demo06b_confluent_source_consumer.py \
  --run-id lec6-demo06b-yourname \
  --max-messages 3
```

06B uses a new isolated group, starts at `earliest`, reads exactly three
records, resolves their writer schema IDs through Schema Registry, and validates
the decoded values with `DatagenOrderV1`.

It deliberately makes zero commits. The inspection group must not alter the
processor group's progress.

Expected result:

```text
Consumed 3 validated Avro source records
Secret-free report: outputs/runs/.../demo06b/report.json
```

In Confluent Cloud, the message viewer should show finite Avro records with
partition, offset, key, and decoded `ORDERS` values:

![Demo 06B connector-created ORDERS messages](assets/demo06/demo06b-topic-messages.jpg)

The value subject should be Avro, versioned, and associated with the input
topic:

![Demo 06B input value schema in Confluent Cloud](assets/demo06/demo06b-topic-schema.jpg)

> ##### STUDENT CHECKPOINT
>
> Which responsibilities belong to the Connect converter, the Avro
> deserializer, and the Pydantic model?

## 8. Demo 06C: bounded stream processor

**Objective:** Turn each durable input event into one durable derived fact while
enforcing the processing order `output acknowledgement -> input offset commit`.

**Why:** Committing first could lose work after a crash. Waiting for the derived
record's broker acknowledgement before committing the input establishes the
demo's explainable at-least-once correctness boundary.

**Done when:** Three inputs produce three acknowledged `OrderMetricV1` records,
each synchronous commit returns the expected next input offset, and the report
shows the acknowledgement-before-commit sequence.

```bash
python demo06c_confluent_stream_processor.py \
  --run-id lec6-demo06c-yourname \
  --max-messages 3
```

For each input, the processor performs this exact sequence:

```text
poll input
  -> Avro deserialize
  -> Pydantic validate
  -> derive OrderMetricV1
  -> Avro serialize
  -> produce to derived topic
  -> wait for output broker acknowledgement
  -> commit the input offset synchronously
```

The output key is the stable source coordinate:

```text
<input-topic>:<partition>:<offset>
```

This makes replayed output identity observable to downstream systems.

> ##### IMPORTANT NOTE
>
> The baseline is at-least-once. If the process crashes after output
> acknowledgement but before the input commit, the input may run again. A
> stable output key supports deduplication, but it does not by itself create a
> universal exactly-once guarantee.

This classroom baseline deliberately flushes each derived record before
committing its input offset so the acknowledgement boundary is visible.
Production processors normally batch acknowledgements or use transactions
instead of paying for one blocking flush per record.

Expected result for the published three-record command:

```text
Processed 3 input records and committed only after output acknowledgement
Secret-free report: outputs/runs/.../demo06c/report.json
```

The verified result card below records an earlier four-record live run. The
published classroom command above uses three records; both runs follow the same
acknowledge-before-commit sequence:

![Demo 06C verified result summary](assets/demo06/demo06c-actual-result.jpg)

Each successful record in the generated report also includes the
broker-confirmed next offset returned by the synchronous commit. A
partition-level commit error stops the run and is never counted as success.

## 9. Demo 06D: same-group resume and new-group replay

**Objective:** Prove two different recovery behaviors: the same consumer group
resumes after its committed position, while a distinct replay group
intentionally rereads retained history.

**Why:** Resume and replay solve different operational problems. Resume supports
normal recovery; replay supports backfills, reprocessing, and verification.
Changing `auto.offset.reset` alone is not a reliable replay command once a group
already has committed offsets.

**Done when:** The first and resume passes use the same group and read disjoint
coordinates, while the replay group is distinct, forces
`OFFSET_BEGINNING`, and covers the first-pass coordinates again.

Use a new run ID and at least six available input records:

```bash
python demo06d_confluent_resume_replay.py \
  --run-id lec6-demo06d-yourname \
  --messages-per-pass 3
```

The script runs three processor passes:

| Pass | Group behavior | Expected input |
|---|---|---|
| First | New base group | First three records |
| Resume | Same base group | Next three records |
| Replay | Distinct replay group with forced beginning | First three records again |

The script fails unless:

- first and resume source coordinates are disjoint;
- replay covers the same first-pass source-coordinate identities, without
  assuming a global order across partitions;
- first and resume use the same group;
- replay uses a distinct group; and
- replay overrides every assigned partition to `OFFSET_BEGINNING`.

`auto.offset.reset=earliest` is only a fallback when a group has no committed
position. It is not a reset command. Demo 06D forces the replay position in
`on_assign`, so reusing the replay group cannot silently turn replay into
resume.

Replay intentionally republishes derived events. The report makes those stable
duplicate keys visible.

Expected result:

```text
Same-group resume and new-group replay checks passed
Secret-free report: outputs/runs/.../demo06d/report.json
```

The following card summarizes a shorter two-record validation run. The command
above remains the three-record classroom default.

![Demo 06D verified resume and forced-replay summary](assets/demo06/demo06d-resume-replay.jpg)

## 10. Responsibility map

| Component | Owns | Does not own |
|---|---|---|
| Kafka Connect | External-system integration, connector/task lifecycle | Arbitrary business processing |
| Converter | Connect data to Kafka key/value representation | Application business rules |
| Kafka | Durable partition logs | Joins, classification, or application validation |
| Schema Registry | Schema identity, versions, compatibility | Business records or consumer offsets |
| Pydantic | Application-level field and rule validation | Kafka delivery |
| Processor | Transformation, state, output, processing order | Connector deployment |
| Consumer offset commit | The group's next input position | Producer delivery or Git history |

## 11. Common mistakes

| Symptom | Cause | Fix |
|---|---|---|
| Connector remains `PROVISIONING` or `FAILED` | Topic, credential, or Schema Registry access is incomplete | Open connector/task status and correct the reported configuration |
| Connector reports an incompatible existing schema | Another schema already owns the selected `<topic>-value` subject | Keep compatibility enabled and use the published fresh Demo 06 topic or a new topic name |
| 06B consumes zero records | Connector did not produce, was paused too early, or fallback was not run | Confirm at least three records in the input topic |
| Pydantic rejects an input | The selected connector quickstart or output format does not match | Use `ORDERS` with `AVRO` |
| 06C processes zero records | Its new group has no accessible input records | Confirm the input topic and group ACLs |
| 06D resume repeats the first batch | The base group ID changed or its commit failed | Keep the generated base group and inspect 06C evidence |
| Replay creates duplicate derived output | This is expected | Compare stable output keys and explain at-least-once behavior |

## 12. Cost and cleanup

After the exercise:

1. delete the Datagen connector;
2. inspect both Demo 06 topics and Schema Registry subjects;
3. download only secret-free reports if needed;
4. delete the classroom cluster when you no longer need it; and
5. revoke an API key created only for this demo when it is no longer needed.

The connector must not remain running after class.

## 13. Final checklist

- [ ] I can explain Connect vs Kafka vs processor.
- [ ] The connector used `ORDERS`, `AVRO`, one task, and a slow bounded classroom rate.
- [ ] I paused the connector after enough records arrived and deleted it after the exercise.
- [ ] 06B consumed and validated exactly three records.
- [ ] 06C acknowledged each output before committing its input.
- [ ] 06D proved same-group resume and new-group replay.
- [ ] My evidence contains no credentials.
- [ ] I cleaned up paid Cloud resources.

## Official references

- [Confluent Cloud Datagen Source Connector](https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html)
- [Kafka Connect concepts and internal state](https://docs.confluent.io/platform/current/connect/index.html)
- [Confluent Python client overview](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Confluent Python transactional API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#transactional-api)
