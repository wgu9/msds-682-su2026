# MSDS 682 — Final Project Proposal Template

## Submission Requirements

- **Submission:** One PDF through Canvas.
- **Length:** No more than **550 words** and no more than **2 pages**. Ideally, use **1 page**.
- **Architecture sketch:** Required and included within the page limit.
- **Team size:** Individual or a team of up to 2 students.

If you work with a partner, contributions to the entire final project—including
this proposal—must be approximately 50–50. Both partners must contribute to the
proposal and be able to explain the complete project. Clearly describe each
partner's responsibilities below.

Replace the prompts in brackets with concise proposal content.

---

## Project Information

**Project title:** [Concise working title]

**Student name(s):** [Name(s)]

**Project format:** [Individual / Two-person team]

**Contribution plan:**

[For an individual project, state that you own all components. For a two-person
team, describe each partner's responsibilities and explain how the overall
contribution—including this proposal—will remain approximately 50–50.]

## 1. Problem Summary

[What specific problem will the project address? Who needs the result, and what
useful output or decision will the project support? Keep the proposed scope
small enough to complete and demonstrate during this course.]

## 2. Planned Data Source and Classification

- **Data source and official URL:** [Source name and link]
- **Data owner:** [Organization or project]
- **Classification:** [Batch / Realtime / Hybrid]
- **Why this classification applies:** [One concise explanation]
- **Access and limitations:** [API key, rate limit, license, cost, privacy, or
  availability concerns]
- **Local fallback:** [Cached sample, fixture, or deterministic replay plan]

## 3. Architecture Sketch

Insert a compact diagram showing the complete data flow. Clearly distinguish
the following two areas.

### Realtime Data Streaming Layer

Show the applicable path and label the direction of data movement:

```text
data source or generator
→ producer, poller, or replay process
→ Kafka topic(s) and event key
→ schema contract and validation
→ consumer and/or stream processor
→ output event or artifact
```

### Other Components

Show any other components that the project needs and how they connect to the
streaming layer. Examples include:

- data generation and simulation details;
- data preparation, transformation, or ETL;
- machine-learning models or inference services;
- analytical dashboards, reports, or notebooks;
- databases or file storage;
- external APIs, tools, or services.

Label which parts are batch, realtime, or hybrid. The diagram may be simple,
but every component and arrow should have a clear purpose.

## 4. Planned Tools and Packages

[List the main tools and packages and map each one to a project responsibility.
Examples might include Python 3.11, `confluent-kafka`, Pydantic, pandas,
scikit-learn, FastAPI, or a visualization package. Include only tools you
currently expect to use.]

## 5. Feasibility Risks and Plan

- **Minimum working demo:** [What is the smallest end-to-end result that can be
  run and inspected?]
- **Primary risks:** [Data access, credentials, cost, time, technical
  complexity, data quality, privacy, or integration risks]
- **Fallback plan:** [How will you reduce scope or use local sample/replay data
  if the preferred plan fails?]
- **Milestones:** [Brief sequence from initial pipeline to final validation]

## 6. AI Use and Disclosure

If AI will be used in the final project or was used to prepare this proposal,
identify:

- the AI tool(s) or model(s);
- which parts AI helped with;
- which parts were completed by the student or team;
- how AI-generated output was verified, tested, rejected, or modified;
- what evidence or evaluation will be retained.

Follow the same AI-use disclosure expectations used in the course assignments.
If no AI was used to prepare this proposal, state: **"No AI was used to prepare
this proposal."**
