# Final Written Report and Code Rubric

The final written package is worth **20 points**. The presentation and proposal
are graded separately. A simple, complete, verifiable project can earn full
credit; extra tools or architectural complexity do not earn points by
themselves.

For a two-person team, both students receive the same written-package score.

## Simple Scoring Rule

For a **3-point row**:

- **3:** Complete, clear, consistent, and verifiable.
- **2:** Mostly complete, with one material gap.
- **1:** Present, but weak or only partly verifiable.
- **0:** Missing or not verifiable.

For a **2-point row**:

- **2:** Complete and verifiable.
- **1:** Partially complete or unclear.
- **0:** Missing or not verifiable.

Grade only the submitted ZIP and its authorized review environment. Leave a
short note for every row that does not receive full credit.

## Rubric: 20 Points

| Category | Full-credit evidence | Points |
|---|---|---:|
| **1. Problem and useful output** | Defines the problem and target user, then identifies one useful, observable output. | **2** |
| **2. Data and event contract** | Documents the data source and limitations; defines the validated Kafka event, key fields, and representative sample. | **3** |
| **3. Kafka implementation** | Submitted code moves records into Kafka, consumes or processes them, and matches the documented architecture. | **3** |
| **4. Result and traceable evidence** | Includes representative input, visible output, and evidence connecting them through one end-to-end run or replay. | **3** |
| **5. Validation or evaluation** | Includes a repeatable test, metric, acceptance check, or evaluation artifact and explains the observed result. | **2** |
| **6. Reproducible review path** | Provides dependencies, configuration, exact run or access steps, expected output, reviewer access when required, and cleanup. | **3** |
| **7. Bounded AI element** | Defines the AI task or AI-assisted workflow, shows representative evidence, explains verification, and states a limitation or fallback. | **2** |
| **8. Report, package, and ownership** | Provides a coherent report and organized, secret-free ZIP; documents individual ownership or approximately equal team contributions. | **2** |
|  | **Base total** | **20** |

## Optional Bonus: Up to 3 Points

Award **1 point each** for clearly labeled, verifiable evidence beyond the
required minimum:

| Bonus | Evidence | Points |
|---|---|---:|
| **Recovery** | Demonstrates one relevant failure, restart, replay, duplicate, invalid-data, or unavailable-dependency case and the resulting handling. | **+1** |
| **Comparison** | Compares two methods or configurations on the same input using a stated metric and evidence-bounded conclusion. | **+1** |
| **Reviewer automation** | Provides one command or script that runs the minimum path, verifies success, and performs or clearly initiates cleanup. | **+1** |
|  | **Maximum bonus** | **+3** |

The maximum raw written-package score is **23 points: 20 base + 3 bonus**.
Bonus cannot replace a missing base requirement. Late-policy deductions are
applied separately.

See the [Final Project Requirements](#/handouts/final-project) for the deadline,
submission structure, review-path rules, and late policy.
