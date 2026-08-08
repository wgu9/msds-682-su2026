# MSDS 682 Final Written Report and Code Rubric

## Scope

The final written report and code package is worth **20% of the course grade**.
This rubric grades only the submitted final ZIP. The proposal and presentation
are graded separately.

The base rubric has **10 buckets worth 2 points each**. Each bucket contains two
atomic 1-point criteria. Up to **3 bonus points** may be earned for evidence that
goes beyond the required minimum. A simple project can earn all 20 base points.
Additional tools or architectural complexity do not earn points by themselves.

For a two-person team, the shared written package receives one rubric score, and
both students receive that same score. Contribution evidence is graded as part
of the shared package. Presentation scores are separate.

## Atomic Scoring Rule

Score every atomic criterion independently:

- **1 point:** Complete, internally consistent, and directly verifiable in the
  submitted package or its authorized review environment.
- **0.5 points:** Present and relevant, but materially incomplete, unclear,
  inconsistent, or only partly verifiable.
- **0 points:** Missing, contradictory, outside the submitted review path, or
  not verifiable.

Only submitted evidence is graded. The reviewer does not infer missing work
from the proposal, presentation, email, or prior conversations. Every score
below 1 point should identify the relevant file or review step and give a short
reason. Do not apply an automatic cascading penalty: a missing item affects
multiple criteria only when it independently prevents verification of each one.

## Base Rubric: 20 Points

| Bucket | Atomic criterion | Points |
|---|---|---:|
| **1. Problem and Intended Result** | **1.1 Problem and user:** The report defines one specific problem and identifies the user, audience, or decision that needs the result. | 1 |
| | **1.2 Intended result and scope:** The package identifies one useful, observable output and keeps the implemented scope appropriate for the course. | 1 |
| **2. Data Source and Event Contract** | **2.1 Data source:** `DATA_SOURCE.md` or an equivalent section identifies the source, owner, official link when applicable, access method, usage constraints, material limitations, and sample or replay method. | 1 |
| | **2.2 Event contract:** The package defines the event schema and validation boundary, identifies important keys or identifiers, and includes at least one representative valid event. | 1 |
| **3. Streaming Architecture** | **3.1 End-to-end architecture:** A diagram or equivalent description traces the implemented path from source or replay through Kafka and processing to the useful output. | 1 |
| | **3.2 Component ownership:** Topics, producers or connectors, consumers or stream processors, stateful operations when applicable, and output responsibilities are named and consistent with the submitted code. | 1 |
| **4. Kafka Implementation** | **4.1 Ingestion or replay:** Submitted code publishes or delivers validated records into the documented Kafka topic or topics through the stated minimum review path. | 1 |
| | **4.2 Processing path:** Submitted code consumes or processes Kafka records and produces the documented result; the implemented behavior matches the architecture and event contract. | 1 |
| **5. End-to-End Result and Evidence** | **5.1 Traceable run evidence:** The package includes representative input, visible output, and evidence connecting both to one end-to-end run or deterministic replay. | 1 |
| | **5.2 Result interpretation:** The report explains what the result means for the target user and does not claim more than the submitted evidence supports. | 1 |
| **6. Validation or Evaluation** | **6.1 Repeatable check:** The package includes runnable tests, metrics, acceptance checks, or an evaluation harness with a stated expected result. | 1 |
| | **6.2 Reported evaluation:** An evaluation artifact reports the observed result, identifies what passed or failed, and supports the conclusion stated in the report. | 1 |
| **7. Reproducible Review Path** | **7.1 Setup and configuration:** The README provides pinned dependencies, configuration instructions, a secret-free `.env.example` when needed, and exact steps or commands for the minimum review path. | 1 |
| | **7.2 Successful reviewer path:** A cloud-based core path provides the required reviewer access and resource inventory, or a non-cloud path provides a locally runnable minimum demo; the package also states expected output and cleanup steps. | 1 |
| **8. Bounded AI Element** | **8.1 AI boundary and evidence:** `AI_USAGE.md` or an equivalent section identifies the bounded AI task, its input and output, and representative evidence of the AI-assisted result or workflow. | 1 |
| | **8.2 Verification and fallback:** The package records what the students accepted or rejected, how the AI output or AI-assisted work was verified, a material limitation, and a fallback method. | 1 |
| **9. Report and Package Quality** | **9.1 Written report:** `report.pdf` explains the problem, data, architecture, implementation, result, evaluation, and citations or source acknowledgments in a coherent technical narrative. | 1 |
| | **9.2 Reviewable package:** The ZIP has one clear top-level project folder, maps any equivalent organization in the README, includes the required code and evidence, and excludes credentials, `.env`, virtual environments, caches, and unrelated large files. | 1 |
| **10. Ownership and Reflection** | **10.1 Contribution record:** An individual project states individual ownership. A two-person project documents meaningful, approximately 50-50 contributions from both students. | 1 |
| | **10.2 Evidence-bounded limitation:** The report identifies at least one specific limitation supported by the current evidence and explains its consequence. | 1 |
|  | **Base total** | **20** |

## Optional Bonus: Up to 3 Points

Bonus evidence must be included in the submitted package, clearly labeled in
the README, and independently verifiable. Bonus points do not replace missing
base requirements. Apply the same 1, 0.5, or 0 atomic scoring rule.

| Bonus criterion | Observable pass condition | Points |
|---|---|---:|
| **B1. Failure and recovery evidence** | Demonstrates one relevant non-happy-path condition, such as invalid data, duplicate delivery, restart, replay, unavailable dependency, or late data, and shows the implemented handling or recovery behavior. | +1 |
| **B2. Comparative evaluation** | Runs two configurations, methods, or baselines on the same input; defines a metric or acceptance rule; reports both results; and gives an evidence-bounded conclusion. | +1 |
| **B3. Reviewer automation** | Provides an automated command or script that starts or runs the minimum path, verifies the expected result, and performs or clearly initiates cleanup, with a captured successful run. | +1 |
|  | **Maximum bonus** | **+3** |

The maximum raw score is **23 points: 20 base points plus 3 bonus points**.
Canvas late-policy deductions are applied separately from the rubric score.

## Grading Clarifications

- Grade the implemented submission, not the ambition of the original proposal.
- Do not award or remove points solely because a project uses a particular
  cloud provider, SQL engine, dashboard, model, or number of Kafka topics.
- Equivalent filenames and folder structures are acceptable when the README
  clearly maps them to the required evidence.
- A recorded or artifact-based result is acceptable when the published review
  contract does not require a live demo. The evidence must still be traceable
  and verifiable.
- If Canvas has not linked a two-person team, reconcile the identical shared ZIP
  across both student records and apply the same written-package score.
- The presentation is not evidence for missing final-ZIP content and is scored
  separately under the
  [Final Presentation Requirements](#/handouts/final-presentation-requirements).
- The official deadline, submission instructions, review-path requirements,
  and late policy remain in the
  [Final Project Requirements](#/handouts/final-project).
