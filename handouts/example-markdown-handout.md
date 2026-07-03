# Example Handout: Reading a Kafka Topic

This is a template handout. Write in **Markdown**, drop the file in `handouts/`,
add one row to the `handouts` list in `script.js`, and it renders here with
syntax-highlighted code.

## When to use a handout

- Lecture notes and reference sheets that outlive a single class.
- Short walkthroughs with runnable code.
- Anything students should be able to read on the site without a download.

For slides or long documents, add a **PDF** instead: put the file in
`handouts/` and set `kind: "pdf"` in the manifest — it opens directly.

## A code example

Code blocks are highlighted automatically. Fence them with a language tag:

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "clickstream",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="handout-demo",
)

for message in consumer:
    print(f"{message.partition}:{message.offset} -> {message.value!r}")
```

Inline code like `bootstrap_servers` is styled too.

## A quick reference table

| Term      | Meaning                                        |
| --------- | ---------------------------------------------- |
| Topic     | Named stream of records                        |
| Partition | Ordered, independently consumable shard        |
| Offset    | Position of a record within a partition        |

> Tip: keep one handout per file so the URL (`#/handouts/<slug>`) stays stable
> and shareable.
