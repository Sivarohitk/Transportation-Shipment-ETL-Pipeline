# Power BI Decision-Support Design

## Status and scope

This is a build specification for a future Power BI semantic model and
three-page report. It is grounded in the Gold tables and late-risk scoring
output that the pipeline currently produces. It is **not** a PBIX file, a
published semantic model, or evidence that Power BI has been implemented.

The repository's sample data and the currently verified Databricks outputs
are synthetic. The report must retain a visible **Synthetic / demonstration
data** label until the model is connected to an approved operational source.

## Source inventory

Use the configured catalog and Gold schema rather than embedding a workspace
name in Power BI. Databricks names therefore have the form
`<catalog>.<gold_schema>.<table>`. Local and EMR runs use the configured Hive
database (normally `curated`) and partitioned Parquet outputs.

| Power BI query | Physical source | Grain | Role in the report |
| --- | --- | --- | --- |
| `Carrier Snapshot` | `dim_carrier` | One row per (`carrier_id`, `p_date`) | Carrier name, SCAC, service mode, active status, and carrier region |
| `Shipments` | `fct_shipment` | One latest-state row per `shipment_id` | Authoritative shipment volume, delivery, transit, cost, route, and shipment exception measures |
| `Delivery Events` | `fct_delivery_event` | One latest-state row per `event_id` | Exception reason, event type, delivery attempt, and event delay detail |
| `Daily KPI` | `kpi_delivery_daily` | One row per (`p_date`, `region_code`, `carrier_id`) | Pre-aggregated daily and regional trend checks |
| `Carrier Performance` | `carrier_performance` | One row per (`p_date`, `carrier_id`, `service_mode`) | Additive carrier/service-mode comparisons |
| `Route Performance` | `route_performance` | One row per (`p_date`, `origin_region_code`, `destination_region_code`, `carrier_id`) | Additive lane comparisons and regional bottlenecks |
| `Exception Summary` | `delivery_exception_summary` | One row per (`p_date`, `event_type`, `carrier_id`, `region_code`) | Daily exception-category summary |
| `Risk Scores` (optional) | `scored_shipments.csv` | One scored row per `shipment_id` for a scoring run | Probability, band, and predicted-late output; this is not currently a managed Gold table |

`agg_shipment_daily` is a valid Gold source, but it is not needed in the
initial report because `Daily KPI` and the specialized carrier/route rollups
cover the proposed visuals. Loading all three daily aggregates would add
overlapping fact tables without adding a management question.

### Supported fields

- `Shipments`: `shipment_id`, `carrier_id`, origin and destination state,
  `origin_region_code`, destination `region_code`, pickup/promised/actual
  timestamps, shipping cost, distance, delivered/on-time/exception flags,
  delay minutes, transit hours, and `p_date`.
- `Delivery Events`: shipment and carrier keys, event type/time/location,
  delay reason, attempt number, delay/exception flags, transit hours, and
  `p_date`.
- `Carrier Performance`: shipment, delivered, on-time, late, exception, and
  first-attempt counts plus transit, delay, cost, distance, and service mode.
- `Route Performance`: the same core shipment/reliability/cost counts at
  origin-region to destination-region lane grain.
- `Exception Summary`: event and shipment counts, exception counts, event
  type, region, average delay, and the exception-event-type flag.
- `Risk Scores`: `shipment_id`, `pickup_ts`, `risk_probability`, `risk_band`,
  `predicted_late`, and (when labels were supplied) `actual_late`.

## Semantic model

### Dimensions

1. **Date** — create a date table spanning the minimum and maximum
   `Shipments[p_date]`, mark it as the model's date table, and add year,
   month, month name, and year-month attributes.
2. **Carrier Current** — create a Power Query reference to `Carrier Snapshot`.
   Group by `carrier_id` to calculate its maximum `p_date`, then inner-join
   those carrier/date pairs back to the snapshot and verify one resulting row
   per carrier. This gives a unique carrier key for relationships. Rename it
   *Carrier Current* so users understand that its name, service mode, region,
   and active status are current attributes applied to historical facts.
3. **Route attributes** — keep origin and destination region directly on
   `Route Performance`; its composite lane grain does not need a separate
   dimension for these three pages. Add a display column such as
   `origin_region_code & " → " & destination_region_code` in Power Query.

A minimal date table is:

```DAX
Date =
ADDCOLUMNS (
    CALENDAR ( MIN ( 'Shipments'[p_date] ), MAX ( 'Shipments'[p_date] ) ),
    "Year", YEAR ( [Date] ),
    "Month Number", MONTH ( [Date] ),
    "Month", FORMAT ( [Date], "MMM" ),
    "Year Month", FORMAT ( [Date], "YYYY-MM" )
)
```

Sort `Date[Month]` by `Date[Month Number]`.

### Relationships

Create one-to-many, single-direction relationships from dimensions to facts:

| One side | Many side | Cardinality / filter direction |
| --- | --- | --- |
| `Date[Date]` | `Shipments[p_date]` | 1:*; Date filters Shipments |
| `Date[Date]` | `Delivery Events[p_date]` | 1:*; Date filters Delivery Events |
| `Date[Date]` | `Daily KPI[p_date]` | 1:*; Date filters Daily KPI |
| `Date[Date]` | `Carrier Performance[p_date]` | 1:*; Date filters Carrier Performance |
| `Date[Date]` | `Route Performance[p_date]` | 1:*; Date filters Route Performance |
| `Date[Date]` | `Exception Summary[p_date]` | 1:*; Date filters Exception Summary |
| `Carrier Current[carrier_id]` | Each fact/aggregate `carrier_id` | 1:*; Carrier Current filters each table |

Do not relate the fact/aggregate tables to one another. In particular, do not
join `Daily KPI`, `Carrier Performance`, and `Route Performance`; their
different grains would introduce ambiguous filters and inflated totals.

For risk reporting, merge `Risk Scores` into a Power Query reference of
`Shipments` on `shipment_id`, retaining all scored rows and checking that the
join does not increase the risk-row count. Name the result `Shipment Risk`.
This supplies carrier and route fields without a bidirectional fact-to-fact
relationship. Relate `Carrier Current[carrier_id]` and `Date[Date]` to the
merged table. Use the date derived from `pickup_ts` for its date relationship.

The latest-snapshot carrier treatment is intentional for this first model.
If carrier attributes become historically variable, replace it with a dated
carrier key; do not silently present current service mode as historical truth.

## Measures

Create a dedicated, otherwise empty `Measures` table and hide raw rate columns
from report authors. The following measures recompute ratios from supported
counts or additive amounts, so totals remain weighted correctly.

### Shipment measures

```DAX
Shipment Volume =
DISTINCTCOUNT ( 'Shipments'[shipment_id] )

Delivered Shipments =
SUM ( 'Shipments'[delivered_flag] )

On-Time Shipments =
SUM ( 'Shipments'[on_time_delivery_flag] )

Late Shipments =
[Delivered Shipments] - [On-Time Shipments]

On-Time Delivery % =
DIVIDE ( [On-Time Shipments], [Delivered Shipments] )

Late Delivery % =
DIVIDE ( [Late Shipments], [Delivered Shipments] )

Exception Shipments =
CALCULATE (
    DISTINCTCOUNT ( 'Shipments'[shipment_id] ),
    'Shipments'[exception_flag] = 1
)

Exception % =
DIVIDE ( [Exception Shipments], [Shipment Volume] )

Average Transit Hours =
AVERAGE ( 'Shipments'[transit_time_hours] )

Average Cost per Mile =
DIVIDE (
    SUM ( 'Shipments'[shipping_cost_usd] ),
    SUM ( 'Shipments'[distance_miles] )
)
```

`On-Time Delivery %` and `Late Delivery %` use delivered shipments as their
denominator. `Exception %` uses all shipments. Do not sum or simply average
stored percentage columns across dates.

### Carrier comparison measures

```DAX
Carrier Shipment Volume =
SUM ( 'Carrier Performance'[shipment_volume] )

Carrier On-Time % =
DIVIDE (
    SUM ( 'Carrier Performance'[on_time_shipments] ),
    SUM ( 'Carrier Performance'[delivered_shipments] )
)

Carrier Late % =
DIVIDE (
    SUM ( 'Carrier Performance'[late_shipments] ),
    SUM ( 'Carrier Performance'[delivered_shipments] )
)

Carrier Exception % =
DIVIDE (
    SUM ( 'Carrier Performance'[exception_shipments] ),
    SUM ( 'Carrier Performance'[shipment_volume] )
)

Carrier Average Cost per Mile =
DIVIDE (
    SUM ( 'Carrier Performance'[total_shipping_cost_usd] ),
    SUM ( 'Carrier Performance'[total_distance_miles] )
)
```

### Route comparison measures

```DAX
Route Shipment Volume =
SUM ( 'Route Performance'[shipment_count] )

Route On-Time % =
DIVIDE (
    SUM ( 'Route Performance'[on_time_shipments] ),
    SUM ( 'Route Performance'[delivered_shipments] )
)

Route Late % =
DIVIDE (
    SUM ( 'Route Performance'[late_shipments] ),
    SUM ( 'Route Performance'[delivered_shipments] )
)

Route Exception % =
DIVIDE (
    SUM ( 'Route Performance'[exception_shipments] ),
    SUM ( 'Route Performance'[shipment_count] )
)

Route Average Cost per Mile =
DIVIDE (
    SUM ( 'Route Performance'[total_shipping_cost_usd] ),
    SUM ( 'Route Performance'[total_distance_miles] )
)
```

### Exception and optional risk measures

```DAX
Exception Events =
CALCULATE (
    COUNTROWS ( 'Delivery Events' ),
    'Delivery Events'[exception_flag] = 1
)

Affected Shipments =
CALCULATE (
    DISTINCTCOUNT ( 'Delivery Events'[shipment_id] ),
    'Delivery Events'[exception_flag] = 1
)

Average Exception Delay Minutes =
CALCULATE (
    AVERAGE ( 'Delivery Events'[delay_minutes] ),
    'Delivery Events'[exception_flag] = 1
)

Scored Shipments =
COUNTROWS ( 'Shipment Risk' )

High-Risk Shipments =
CALCULATE (
    COUNTROWS ( 'Shipment Risk' ),
    'Shipment Risk'[risk_band] IN { "HIGH", "CRITICAL" }
)

High-Risk % =
DIVIDE ( [High-Risk Shipments], [Scored Shipments] )

Average Risk Probability =
AVERAGE ( 'Shipment Risk'[risk_probability] )

Predicted-Late Shipments =
SUM ( 'Shipment Risk'[predicted_late] )
```

The model defines HIGH and CRITICAL according to the pipeline's existing risk
bands. These measures describe model output; they are not claims about model
accuracy and must not be shown when `Risk Scores` has not been loaded.

## Report page 1 — Executive Supply Chain Overview

**Management decision:** Decide where leadership attention and operating
capacity are needed by comparing volume, reliability, exceptions, transit,
and cost over the selected period.

| Proposed visual | Source and grain | Dimensions / measures | Intended business question |
| --- | --- | --- | --- |
| Six KPI cards | `Shipments`; shipment grain | Shipment Volume, On-Time Delivery %, Late Delivery %, Exception %, Average Transit Hours, Average Cost per Mile | Is overall network performance meeting delivery and cost expectations? |
| Monthly trend lines | `Shipments`; shipment grain | Date[Year Month]; On-Time Delivery %, Late Delivery %, Exception % | Is reliability improving or deteriorating over time? |
| Volume and on-time combo chart | `Shipments`; shipment grain | Date; Shipment Volume (columns), On-Time Delivery % (line) | Are service changes associated with a change in workload? |
| Regional performance matrix | `Daily KPI`; date/region/carrier grain | Destination `region_code`, carrier; supported KPI columns displayed at row grain | Which destination regions and carriers require investigation? |

Use slicers for Date, Carrier Current carrier name/ID, service mode (where
applicable), and destination region. Do not show `Daily KPI` percentage fields
as grand totals; use them only at their stored grain or use shipment measures
for totals.

## Report page 2 — Carrier & Route Performance

**Management decision:** Select carrier and lane interventions by balancing
reliability, cost, volume, service mode, and geographic concentration.

| Proposed visual | Source and grain | Dimensions / measures | Intended business question |
| --- | --- | --- | --- |
| Carrier scorecard matrix | `Carrier Performance`; date/carrier/service-mode grain | Carrier, service mode; volume, on-time %, late %, exception %, cost/mile | Which carriers and service modes are strong or weak? |
| Cost-versus-reliability scatter | `Carrier Performance`; date/carrier/service-mode grain | Carrier as detail; cost/mile on X, on-time % on Y, volume as bubble size, service mode as legend | Which carriers offer the best cost/reliability trade-off at meaningful volume? |
| Lane heat map (matrix with conditional color) | `Route Performance`; date/origin-region/destination-region/carrier grain | Origin rows, destination columns; Route Late % or Route On-Time % | Which origin-to-destination region lanes are bottlenecks? |
| Route drill table | `Route Performance`; same grain | Route display, carrier, volume, transit, exception %, cost/mile | Which carrier-lane combinations should be reviewed first? |

Carrier performance does not contain route regions, and route performance does
not contain service mode. Service-mode visuals must therefore use `Carrier
Performance`; lane visuals must use `Route Performance`. A service-mode-by-lane
visual is unsupported by the current Gold aggregates and should not be added
unless it is calculated from `Shipments` joined to carrier attributes with a
validated historical treatment.

## Report page 3 — Delivery Risk & Exceptions

**Management decision:** Prioritize proactive shipment follow-up and identify
repeatable exception causes without confusing predicted risk with observed
operational failures.

| Proposed visual | Source and grain | Dimensions / measures | Intended business question |
| --- | --- | --- | --- |
| High-risk KPI cards | Optional `Shipment Risk`; scored-shipment grain | High-Risk Shipments, High-Risk %, Predicted-Late Shipments | How many scored shipments need proactive review? |
| Risk-band distribution | Optional `Shipment Risk`; scored-shipment grain | risk_band; Scored Shipments | How is the scored population distributed across risk bands? |
| Risk by carrier | Optional merged `Shipment Risk`; scored-shipment grain | Carrier; Average Risk Probability and High-Risk % | Which carriers have the greatest concentration of model-flagged shipments? |
| Risk by route | Optional merged `Shipment Risk`; scored-shipment grain | Origin region, destination region; Average Risk Probability and High-Risk % | Which lanes have the greatest concentration of model-flagged shipments? |
| High-risk shipment table | Optional merged `Shipment Risk`; scored-shipment grain | shipment ID, carrier, route, pickup/promise timestamps, probability, band, predicted-late | Which individual shipments should operations review? |
| Exception-category bars | `Exception Summary`; date/event-type/carrier/region grain | event_type; exception_event_count | Which operational event categories generate exceptions? |
| Delay-reason pattern chart | `Delivery Events`; event grain | delay_reason and Date; Exception Events, Affected Shipments, Average Exception Delay Minutes | Which stated reasons drive exception volume and delay severity? |

Sort risk bands with an explicit numeric order: LOW = 1, MEDIUM = 2, HIGH = 3,
CRITICAL = 4. Validate `risk_probability` is numeric and between 0 and 1,
`risk_band` is populated, and `predicted_late` is 0/1 during Power Query load.
Do not drop scored rows with null carrier or route attributes; label those
dimensions `Unknown` so scoring coverage remains auditable.

## Connection and import instructions

### Databricks managed Gold tables

The live Databricks path is the preferred source for the managed Gold model:

1. In the target workspace, open the SQL warehouse and copy its **Server
   Hostname** and **HTTP Path** from Connection details. Do not save these or
   any token in the repository.
2. In the latest Power BI Desktop, choose **Get data**, search for
   **Databricks**, and select the available Databricks connector.
3. Enter the server hostname and HTTP path. Choose **Import** for this initial
   small, synthetic model. Use DirectQuery only when freshness/scale requires
   it and the SQL warehouse is intended to support interactive BI load.
4. Authenticate through the organization-approved option (interactive OAuth,
   personal access token, or service-principal credentials as available).
   Credentials belong in Power BI's credential store, never in source control.
5. In Navigator, select the configured catalog and Gold schema, then load only
   the tables in the source inventory. Confirm the user has catalog/schema/table
   read permissions.
6. Apply correct data types in Power Query, build `Carrier Current`, create the
   relationships above, and reconcile imported row counts to Databricks SQL
   before authoring visuals.

Microsoft and Databricks both document the server-hostname/HTTP-path workflow,
supported authentication choices, and Import/DirectQuery selection in their
current connector guides: [Microsoft Power BI Desktop connection
guide](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi/desktop)
and [Databricks on AWS Power BI Desktop connection
guide](https://docs.databricks.com/aws/en/partners/bi/power-bi/desktop).

The optional `Risk Scores` source is currently a CSV in the configured ML
output directory under the Databricks audit Volume, not a Unity Catalog Gold
table. The Databricks table Navigator will not expose it as a Gold table.
For a manual demonstration, download the verified `scored_shipments.csv` and
load it with **Text/CSV**, or omit risk visuals. Do not claim scheduled Power BI
refresh for this file. A governed scoring table would require a future,
separately tested pipeline change.

### Local outputs

1. Complete a local pipeline run and use the configured
   `paths.curated_base_path` (normally `data/local/curated`). Each table is a
   partitioned Spark output folder, not one guaranteed file.
2. In Power BI Desktop, choose **Get data > Folder** for each required table
   directory, retain Parquet part files, and combine them with Power Query's
   `Parquet.Document` transformation. Ignore Spark marker files such as
   `_SUCCESS`.
3. Spark partition columns can live in folder names rather than inside each
   Parquet part. If they are absent after combining, derive `p_date`,
   `region_code`, and `carrier_id` from the `Folder Path` segments before
   removing that column.
4. A Windows development run can fall back to partitioned JSON when the local
   Hadoop/winutils limitation prevents a Parquet write. In that case, use the
   Folder/JSON connector and apply the same explicit types and partition-path
   extraction. Do not mix Parquet and JSON parts in one query.
5. Load `data/scored/scored_shipments.csv` with **Text/CSV** only if the ML run
   that produced it has completed successfully, then perform the risk merge
   and validation described above.

Power Query's documented Parquet connector supports Import from the local file
system: [Power Query Parquet connector](https://learn.microsoft.com/en-us/power-query/connectors/parquet).
Publishing a semantic model whose source is local files will require an
appropriately configured gateway and refresh credentials; that deployment is
outside this phase.

### EMR outputs

EMR writes the same logical model as S3-backed partitioned Parquet/Hive tables.
This design preserves those schemas, but this phase does not configure or
claim a Power BI-to-S3/EMR connection. Use an organization-approved accessible
query endpoint or copy/export process before building the semantic model;
never embed AWS credentials in the PBIX or repository.

## Manual build acceptance checks

Before calling a report implemented, the report author must:

- reconcile table and distinct business-key counts to the selected source;
- confirm `Shipments[shipment_id]` and optional `Risk Scores[shipment_id]` are
  unique at their documented grains;
- confirm every dimension-to-fact relationship has the expected cardinality
  and single-direction filtering;
- validate percentage totals against direct source queries at date, carrier,
  region, and route slices;
- verify zero-distance rows do not produce infinite cost-per-mile values;
- verify null transit times are excluded rather than converted to zero;
- retain unmatched risk rows and expose them as Unknown carrier/route;
- label the demonstrated data synthetic; and
- manually test page filters, cross-highlighting, drill behavior, refresh, and
  credential handling before publishing.

No screenshots or PBIX artifacts are included because no Power BI report has
yet been manually created or validated.
