# DMAIC Process-Improvement Case Study

## Scope and evidence boundary

This portfolio case study applies the DMAIC structure to the transportation
lakehouse implemented in this repository. It does **not** claim Six Sigma
certification, a completed consulting engagement, or measured improvement at
a real company.

The project's sample and live-validation inputs are synthetic. Metrics shown
by the pipeline are real calculations over those inputs, but they are not a
production logistics baseline. Proposed operational actions below have not
been deployed or proven to improve business performance.

Two labels keep that boundary explicit:

- **IMPLEMENTED** — code, tables, metrics, validation, or report design that
  exists in this repository and, where stated, was exercised in the
  development Databricks workspace.
- **PROPOSED BUSINESS ACTION** — an operational decision or control practice
  that management could adopt after validating the evidence with production
  data. It is not implemented by this project.

## DEFINE

### Business problem

**PROPOSED BUSINESS PROBLEM**

A transportation operation cannot consistently determine where unreliable
on-time delivery is concentrated. Late deliveries and exceptions may be
associated with particular carriers, region-to-region routes, service modes,
or exception categories, but fragmented raw records make those patterns hard
to compare. This delays intervention and can encourage carrier or routing
decisions based on anecdotes rather than measured delivery, transit, and cost
performance.

This is a concrete improvement problem for the case study, not a claim that
the synthetic dataset proves a real organization's on-time delivery is below
target.

### Affected stakeholders

- Transportation and logistics managers who select carriers and review lanes.
- Dispatch and operations teams who follow shipments and resolve exceptions.
- Carrier-management and procurement teams balancing reliability and cost.
- Regional operations leaders investigating geographic bottlenecks.
- Data and BI teams responsible for consistent KPI definitions and refreshes.
- Customers and receiving locations affected by late or disrupted deliveries.

### Operational objective

**PROPOSED BUSINESS ACTION**

Improve delivery reliability by increasing the on-time delivery rate and
reducing late deliveries and operational exceptions, while monitoring transit
time and cost per mile so that service gains are not pursued without visibility
into cost and speed trade-offs.

No numeric target is asserted because the project does not contain an approved
business SLA, control limit, or production baseline. Targets should be set by
management only after a representative measurement period is loaded and the
metric definitions below are accepted.

### Implemented analytical scope

**IMPLEMENTED**

The pipeline supports the problem statement with shipment, carrier, and
delivery-event ingestion; Bronze/Silver/Gold processing; managed Delta tables
on Databricks; Parquet/Hive-compatible local and EMR outputs; daily KPI tables;
carrier, route, and exception analytics; and a synthetic late-shipment
decision-support model.

## MEASURE

### Operational definitions

**IMPLEMENTED**

| Measure | Implemented definition | Primary Gold source and grain |
| --- | --- | --- |
| Shipment volume | Distinct shipment count | `fct_shipment`, one latest-state row per `shipment_id`; or `carrier_performance.shipment_volume` at daily carrier/service-mode grain |
| On-time delivery rate | On-time delivered shipments divided by delivered shipments | `kpi_delivery_daily` at (`p_date`, `region_code`, `carrier_id`); `carrier_performance`; `route_performance` |
| Late delivery rate | Late delivered shipments divided by delivered shipments | `kpi_delivery_daily`; `carrier_performance`; `route_performance` |
| Average transit time | Average pickup-to-delivery duration in hours | `fct_shipment.transit_time_hours` and the carrier/route/KPI rollups |
| Exception rate | Exception shipments divided by shipment volume | `kpi_delivery_daily`; `carrier_performance`; `route_performance` |
| Average cost per mile | Total shipping cost divided by total distance | `fct_shipment` and the carrier/route/KPI rollups |
| Carrier performance | Volume, delivered/on-time/late/exception counts and rates, first-attempt success, transit, delay, cost, and distance by date/carrier/service mode | `carrier_performance`, one row per (`p_date`, `carrier_id`, `service_mode`) |
| Route performance | Volume, reliability, exceptions, transit, cost, and distance by date/origin region/destination region/carrier | `route_performance`, one row per (`p_date`, `origin_region_code`, `destination_region_code`, `carrier_id`) |
| Exception-category frequency | Event and affected-shipment counts, exception counts, rate, and average delay by event type | `delivery_exception_summary`, one row per (`p_date`, `event_type`, `carrier_id`, `region_code`) |

On-time and late rates use delivered shipments as the denominator. Exception
rate uses all shipments. Cost per mile is calculated from summed cost divided
by summed distance, rather than averaging row-level ratios. These denominator
rules prevent misleading totals when dates or operating segments are combined.

### How the pipeline establishes trustworthy measurement

**IMPLEMENTED**

1. **Explicit contracts at ingestion.** Shipment, carrier, and delivery-event
   CSVs are read with JSON schema contracts rather than schema inference.
2. **Visible invalid records.** Invalid records are quarantined with entity,
   rule, and run context instead of being silently discarded.
3. **Silver standardization and validation.** The pipeline checks required
   values, allowed values, timestamp ordering, non-negative measures, schema
   drift, and duplicate business keys before Gold publication.
4. **Latest-state business grains.** Shipment and delivery-event facts retain
   one latest state per business key; Databricks uses Delta MERGE for changing
   operational records.
5. **Configuration-driven publication.** Local and EMR retain Parquet/Hive
   behavior, while the Databricks profile writes managed Delta tables with
   configured Unity Catalog names.
6. **Repeatable KPI definitions.** Gold builders publish documented grains and
   compute carrier, route, regional, exception, transit, and cost measures from
   the conformed facts.
7. **Idempotency evidence.** The development Databricks validation reran the
   pipeline without uncontrolled duplicate business keys and verified a
   deterministic Silver MERGE update.
8. **Data-quality execution.** The Lakeflow workflow has a task that runs the
   repository's existing data-quality tests and fails on a nonzero pytest
   result. In the verified development workflow, that task completed with 11
   passing checks.

The live development validation queried 14 managed tables: three Bronze,
three Silver, and eight Gold. It used synthetic data, so this evidence supports
technical measurement integrity—not a business-performance conclusion.

### Baseline procedure

**PROPOSED BUSINESS ACTION**

Before setting improvement targets, management should select a representative
production period and record:

- overall and weekly on-time, late, and exception rates;
- average transit hours and cost per mile;
- shipment volume and denominator counts for every rate;
- the same measures by carrier, service mode, route, and region; and
- exception-event volume, affected shipments, and average delay by category.

The measurement window, filters, excluded/quarantined record counts, and data
refresh timestamp should accompany the baseline. Small-volume segments should
not be ranked as if their rates were as stable as high-volume segments.

## ANALYZE

### Gold analytical views

**IMPLEMENTED**

The Gold layer supports a drill path from network outcomes to operational
segments:

| Analysis | Implemented source | Evidence it can expose |
| --- | --- | --- |
| Carrier comparison | `carrier_performance` | Carriers with high volume but lower on-time performance, higher exception rate, longer transit, or higher cost per mile |
| Route/lane comparison | `route_performance` | Origin-to-destination region lanes with concentrated lateness, exceptions, long transit, or high cost |
| Regional comparison | `kpi_delivery_daily`, `route_performance`, and `delivery_exception_summary` | Destination-region outcomes and directional regional bottlenecks |
| Service-mode comparison | `carrier_performance.service_mode` | Differences among FTL, LTL, Parcel, or Unknown service-mode groups |
| Exception-category comparison | `delivery_exception_summary` and `fct_delivery_event.delay_reason` | Frequent exception event types, affected shipments, delay severity, and recorded delay reasons |
| Shipment detail | `fct_shipment` and `fct_delivery_event` | Individual shipment and event records supporting aggregate investigation |

An analyst can begin with on-time or late performance, retain the associated
volume, then compare exception rate, transit time, and cost per mile. This
guards against selecting a carrier or route solely because of a favorable
percentage calculated over very few shipments.

These comparisons identify associations and candidates for investigation.
They do not demonstrate that a carrier, route, service mode, region, or event
category caused an outcome. Weather, facility congestion, customer readiness,
and other external factors are not represented in the implemented Gold model.

### ML and statistical evidence

**IMPLEMENTED**

The late-shipment model produces a probability, LOW/MEDIUM/HIGH/CRITICAL risk
band, and `predicted_late` flag for each scored shipment. Booking-time features
include carrier, service mode, origin/destination context, promised transit,
distance, cost, and strictly chronological carrier/route history. Post-delivery
fields are blocked by the leakage audit.

The documented 5,000-shipment synthetic run used a chronological 70/15/15
train/validation/test split. On the held-out test set, logistic regression
reported ROC-AUC 0.558, PR-AUC 0.253, precision 0.200, recall 1.000, and F1
0.333 at the default 0.25 decision threshold. The gradient-boosting model
reported test ROC-AUC 0.531 and showed substantial training-to-test overfit.
Those observed results indicate weak discrimination and many false-positive
reviews; they do not justify automated routing or carrier-selection decisions.

The development Databricks ML task successfully scored all 5,000 synthetic
rows, including deliberately supplied rows with a null route component.
Probabilities were finite and within `[0, 1]`, and every row had a populated
risk band and predicted-late value.

**ANALYTICAL USE, NOT CAUSATION**

Risk output can help order a manual review queue or suggest segments for
further analysis. Model coefficients, probabilities, historical late rates,
and segment differences are predictive associations only. They do not prove
that changing a carrier or route will cause on-time delivery to improve.

## IMPROVE

No intervention in this section has been deployed by the project.

### Carrier and service selection

**PROPOSED BUSINESS ACTION**

- Review high-volume carrier/service-mode segments with persistently weaker
  on-time performance or higher exception rates.
- Compare reliability with weighted cost per mile and transit time before
  changing allocation; do not optimize one KPI in isolation.
- Pilot a limited volume shift only after validating that compared segments
  serve comparable lanes and shipment profiles.
- Define the pilot duration, eligible shipments, expected outcome, and guardrail
  measures before execution.

### Route attention

**PROPOSED BUSINESS ACTION**

- Prioritize high-volume origin-to-destination regional lanes with elevated
  late or exception rates.
- Use shipment and event detail to determine whether the issue is concentrated
  at an origin, destination, carrier, or recorded exception reason.
- Test operational changes on a bounded lane population and compare the same
  delivery, transit, exception, and cost measures before and after the trial.

### Proactive high-risk shipment review

**PROPOSED BUSINESS ACTION**

- Present HIGH and CRITICAL scored shipments to an operator as a review queue,
  not as an automated decision.
- Confirm route/carrier context and contact status before escalating.
- Record the review effort, intervention, and observed outcome so precision,
  recall, and business value can later be re-evaluated on real data.
- Recalibrate or suspend the queue if false-positive workload outweighs the
  avoided-delay value. The current synthetic model's weak held-out performance
  makes this guardrail essential.

### Exception prioritization

**PROPOSED BUSINESS ACTION**

- Rank exception categories using event count, distinct affected shipments,
  average delay, and associated shipment volume.
- Investigate repeatable process categories before isolated events.
- Assign an operational owner and a measurable response for a selected
  category, then evaluate whether its frequency and delay severity change.

### Improvement evaluation

**PROPOSED BUSINESS ACTION**

Use a documented pre/post or controlled-pilot design. Keep metric definitions,
population filters, and observation windows consistent; report both counts and
rates; and retain cost and transit guardrails. A before/after difference alone
does not establish causation when volume mix, season, carrier mix, or routes
also changed.

## CONTROL

### Implemented control foundation

**IMPLEMENTED**

- The Databricks bundle defines a three-task Lakeflow workflow:
  `ingest_bronze_silver_gold`, `score_late_risk`, and
  `data_quality_checks`, with explicit dependencies and failure propagation.
- Scheduled pipeline execution at 06:00 UTC daily is defined in the bundle but
  is currently **PAUSED**. Recurring production execution has not been claimed.
- Gold KPI monitoring is supported by daily network, carrier, route, regional,
  and exception measures at documented grains.
- The data-quality task runs 11 repository checks in the verified development
  workflow. It currently tests the synthetic data-quality suite; it does not
  query all newly written Unity Catalog tables as a post-run reconciliation.
- The Power BI decision-support document specifies three report pages,
  conformed dimensions, valid weighted measures, relationships, visuals, and
  manual acceptance checks. No PBIX file or published report exists.
- Databricks managed-table MERGE and rerun idempotency were validated in the
  development workspace with synthetic inputs.

### Proposed operating controls

**PROPOSED BUSINESS ACTION**

1. Approve a production refresh frequency and activate scheduling only after
   production catalog, input, permission, and support requirements are met.
2. Treat a failed ETL or data-quality task as a blocked KPI refresh; do not
   publish stale or partially updated results as current performance.
3. Build the documented Power BI report manually and reconcile table counts,
   business-key uniqueness, percentage denominators, and filter behavior to
   source queries before publishing it.
4. Display last-successful-refresh time and synthetic/production data status
   prominently in the report.
5. Review on-time, late, exception, transit, and cost measures on a defined
   cadence at network, carrier, route, region, and service-mode levels.
6. Establish management-approved targets and warning limits from a production
   baseline. The portfolio example thresholds in other documentation are not
   implemented control limits.
7. Investigate threshold breaches with shipment/event detail and quarantined
   record counts before assigning operational cause.
8. Track intervention owner, start date, eligible population, and expected
   measure; compare results over a sufficiently representative window.
9. Monitor scored-row coverage, probability validity, risk-band population,
   model precision/recall, and operator workload before using ML risk in an
   operational control process.

### Control response plan

**PROPOSED BUSINESS ACTION**

| Signal | Review | Potential response | Guardrail |
| --- | --- | --- | --- |
| On-time rate deterioration | Delivered denominator, volume mix, carrier, lane, and region | Open a bounded carrier/lane investigation | Do not infer cause from the aggregate alone |
| Late or transit increase | Promise timestamps, delayed shipments, and event timeline | Review handoff, route, or capacity process | Compare cost and shipment mix |
| Exception increase | Event type, delay reason, affected shipments, and quarantine counts | Prioritize a repeatable exception category | Separate data defects from operating events |
| Cost-per-mile increase | Total cost, total miles, carrier, route, and service mode | Review allocation and rate drivers | Do not trade away reliability without approval |
| High-risk queue growth | Score coverage, band distribution, threshold metrics, and drift | Adjust manual review capacity or reassess threshold/model | No automated action; validate on real labels |
| Pipeline or quality failure | Lakeflow task result and failed assertion | Hold the reporting refresh and remediate data/process issue | Never hide or bypass the failed check |

## Implemented versus proposed summary

| IMPLEMENTED | PROPOSED BUSINESS ACTION |
| --- | --- |
| Bronze/Silver/Gold shipment, carrier, and event pipeline | Establish a production business baseline and approved targets |
| Documented on-time, late, exception, transit, cost, carrier, and route measures | Change carrier/service allocation after a controlled review |
| Gold carrier, route, regional KPI, and exception analytical tables | Pilot corrective action on selected routes or exception categories |
| Schema enforcement, quarantine, Silver checks, business-key handling, and Databricks data-quality task | Activate and operationally support a production schedule |
| Databricks development deployment, end-to-end synthetic run, MERGE, and idempotency validation | Build, validate, publish, and govern the Power BI report |
| Chronological synthetic risk modeling and scored decision-support output | Use high-risk scores in a supervised review process and validate value on production labels |
| Power BI semantic/report build specification | Claim sustained improvement only after an appropriately designed evaluation |

## Conclusion

The implemented platform supplies consistent measurements and drill paths for
a DMAIC-style delivery-reliability investigation. It does not establish a real
company baseline, prove root cause, deploy an intervention, or demonstrate
sustained improvement. Those outcomes require production data, management-
approved targets, controlled operational action, and ongoing validation.
