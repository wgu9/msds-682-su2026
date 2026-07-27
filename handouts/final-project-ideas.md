# Final Project Ideas

These examples are starting points, not assigned topics. You may adapt one,
combine parts of several, or propose a different project. Choose a problem that
you can explain clearly and reduce to one complete Kafka path with an observable
result. A smaller system that another person can run and verify is stronger than
a large design that remains unfinished. Each idea below includes one possible
bounded AI component; you may replace it with another disclosed and evaluated
AI-assisted workflow. Before submitting the proposal, review the
[Final Project requirements](#/handouts/final-project), the
[Proposal Template](#/handouts/final-project-proposal-template), and the
[10-Point Proposal Rubric](#/handouts/final-project-proposal-rubric).

## Five possible directions

**1. Wikimedia Recent Changes Monitor · realtime · low to medium difficulty.**
Build a monitoring tool for editors, researchers, or moderators using the
[Wikimedia Recent Changes EventStream](https://stream.wikimedia.org/v2/stream/recentchange).
A producer reads the public server-sent event stream, validates a small event
schema, and publishes selected changes to Kafka; a Python consumer then computes
windowed counts by wiki, page, or change type and produces an alert or compact
report. A bounded AI component could classify a small subset of edits or
summarize only the evidence contained in selected events, with labeled examples
used to evaluate it. Keep a short cached JSONL sample so the same pipeline can
be replayed when the live feed or network is unavailable.

**2. USGS Earthquake Alert and Situation Summary · near-realtime · medium
difficulty.** Create an alerting service for a traveler, emergency-planning
team, or local news desk using the
[USGS Earthquake GeoJSON feeds](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).
A poller checks one official feed at a reasonable interval, validates each
record, deduplicates events by USGS event ID, and sends new or updated events to
Kafka; a consumer applies transparent magnitude and location rules and writes an
alert artifact. A bounded AI component could turn the structured facts into a
short situation summary, tested for unsupported claims and numeric accuracy.
Use saved API responses as a deterministic replay path, and do not depend on an
earthquake occurring during the demonstration.

**3. Citi Bike Station Availability Monitor · hybrid · medium difficulty.**
Design an operations tool for a bike-share dispatcher or commuter using the
realtime GBFS feeds and historical trip files linked from the
[Citi Bike System Data page](https://citibikenyc.com/system-data). Publish
station-status snapshots to Kafka keyed by station ID, maintain the latest
availability state, and emit a CSV, alert stream, or dashboard-ready file for
stations that are nearly empty or full; a small historical sample can provide
context or replay data. A bounded AI component could explain a proposed
rebalancing action from the calculated station facts, and an evaluation should
check whether the explanation preserves those facts. Limit the first version to
one feed, one borough or station subset, and cached snapshots for offline use.

**4. NYC Taxi Demand Replay and Anomaly Report · batch replay · medium
difficulty.** Build a demand-monitoring pipeline for a transportation analyst
using one month or a small fixed sample from the
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
A replay producer emits trips in timestamp order at an accelerated rate, Kafka
events are keyed by pickup zone, and a consumer computes bounded time-window
metrics such as trip count, fare, or tip rate before writing an anomaly report
or dashboard-ready dataset. A bounded AI component could explain already
detected anomalies, while an evaluation compares every explanation with the
underlying metrics. Do not download many months or make a live dashboard the
minimum success path; a fixed sample and a reproducible output file are enough.

**5. MovieLens Streaming Recommendations · batch replay · medium to high
difficulty.** Create a small recommendation service for a movie application
using a research dataset from the
[GroupLens MovieLens page](https://grouplens.org/datasets/movielens/). Replay a
bounded set of ratings as user events, publish them to Kafka keyed by user ID,
and maintain simple state such as recent ratings, movie popularity, or
item-to-item co-occurrence before producing a top-N recommendation artifact. A
bounded AI component could explain a recommendation using only the computed
evidence, with tests for unsupported movie facts and consistency with the
ranking. Start with a smaller MovieLens edition or sampled records, cite
GroupLens, follow the dataset's usage terms, and do not redistribute the
original dataset inside the submission unless its terms permit it.

## Keep the proposal course-sized

- Name one target user, one useful output, and one minimum end-to-end demo.
- Show where validation, Kafka, state or windowing, AI, and evaluation enter the
  data flow; extra technologies do not earn extra proposal points.
- Test data access early and include a local sample, replay, or scope-reduction
  fallback in the proposal.
- Record the owner, link, license, and usage limits of your chosen source; each
  of these datasets has its own terms, and the final package documents them in
  `DATA_SOURCE.md`.
