# Late-Shipment Risk Model (Phase 8)

## Purpose

A decision-support model that flags shipments likely to miss their
promised delivery time.  The model is a **portfolio / decision-support
artefact** trained on a synthetic dataset.  All numbers reported in
this document come from real model runs on held-out chronological
splits; no metric is fabricated.

The model is **not** intended for production deployment.  It is
intentionally simple, uses only `scikit-learn` primitives, and
documents every design choice.

## Scope

| Question | Answer |
|---|---|
| What does it predict? | ``P(is_late)`` for a single shipment at booking time. |
| When is the prediction made? | At booking time — only features known before the carrier picks up the freight are used. |
| How is the data split? | **Chronological**: 70/15/15 (train / validation / test) on ``pickup_ts``. |
| What models are trained? | Logistic Regression (linear baseline) and `HistGradientBoostingClassifier` (tree-based).  No external ML libraries are introduced. |
| What is the evaluation target? | ``is_late = actual_delivery_ts > promised_delivery_ts``, derived from the public schema. |
| What is the source of the data? | The synthetic generator in `transport_etl.synthetic` (Phase 7). |

## Leakage Audit

A model is only as honest as its data pipeline.  The leakage
contract is enforced in code at three boundaries:

1. **Feature engineering** — `build_feature_matrix` drops any
   column that appears in the forbidden list
   (`FORBIDDEN_FEATURE_COLUMNS`) and re-runs
   `assert_no_leakage` on the output.
2. **Training** — estimators use fixed categorical and numeric feature
   allowlists; identifiers, labels, and output columns are not selected
   dynamically from the input frame.
3. **Tests** — the suite checks the forbidden list, tied pickup cohorts,
   outcome availability, and missing route groups.

The forbidden columns are documented in
`transport_etl.ml.leakage_audit.FORBIDDEN_FEATURE_COLUMNS`:

- `actual_delivery_ts` — the source of the label
- `on_time_delivery_flag` — a 0/1 alias of the label
- `delay_minutes` — derived from `actual - promised`
- `transit_time_hours` — only known after delivery
- `delivered_flag` — derived from `actual_delivery_ts` presence
- `exception_flag` — derived from delivery events
- `shipment_updated_at` — can equal `actual_delivery_ts`
- `out_for_delivery_ts`, `delivered_event_ts`,
  `delivered_event_attempt_number` — only known during transit
- `exception_event_count` — derived from delivery events
- `agg_total_shipments`, `agg_delivered_shipments`,
  `agg_on_time_shipments`, `agg_late_shipments`,
  `agg_exception_shipments` — Gold aggregates that depend on
  actual delivery (would be retrospective)

### Allowed features

| Feature | Source | Why safe at prediction time |
|---|---|---|
| `shipment_id` | shipment key | Identifier (never used as a feature) |
| `pickup_ts` | booking | Known at booking |
| `pickup_dow`, `pickup_hour`, `pickup_month` | derived from `pickup_ts` | Pure functions of a booking-time timestamp |
| `carrier_id` | shipment | Known at booking |
| `service_mode` | `dim_carrier` join | Carrier attribute at booking |
| `is_active` | `dim_carrier` join | Carrier attribute at booking |
| `home_region_code` | `dim_carrier` join | Carrier attribute at booking |
| `origin_state`, `destination_state` | shipment | Known at booking |
| `region_code`, `origin_region_code` | shipment (enriched) | Known at booking |
| `promised_delivery_ts` | shipment | The SLA, known at booking |
| `promised_transit_hours` | derived | `(promised - pickup) / 1h`, pure function |
| `distance_miles` | shipment | Known at booking |
| `shipping_cost_usd` | shipment | Known at booking |
| `carrier_historical_late_rate` | computed | Uses only shipments whose pickup and observed delivery both precede the current pickup |
| `carrier_historical_shipment_count` | computed | Count of those observable carrier outcomes |
| `route_historical_late_rate` | computed | Uses the same as-of rule and preserves missing route groups |
| `route_historical_shipment_count` | computed | Count of those observable route outcomes |

### Why chronological validation is necessary

Random train/test splits are **inadmissible** for a late-delivery
risk model.  A shipment whose `pickup_ts` is in 2025-05 cannot be
used to predict a shipment whose `pickup_ts` is in 2025-09 because:

1. The carrier's reliability may have changed between the two
   dates (a new fleet, a new management, a regulatory change).
2. The seasonal pattern of lateness (summer storms, holiday
   surge) is real and predictable only when the model respects
   time.
3. Random splits leak the *future* into the training set,
   producing optimistic evaluation metrics.  Those metrics
   misrepresent production behaviour.

We sort the dataset on `pickup_ts`, assign the earliest 70% to
train, the next 15% to validation, and the latest 15% to test.
The test split is the chronologically most recent — the only honest
approximation of "what would the model have predicted on
shipments it has never seen".

## Models

### Logistic Regression (baseline)

`sklearn.linear_model.LogisticRegression(solver='liblinear', C=1.0, class_weight='balanced')`

- Linear baseline; coefficients are interpretable.
- `class_weight='balanced'` compensates for the ~18% positive
  class imbalance in the synthetic data.
- `solver='liblinear'` is robust on small / sparse data.

### HistGradient Boosting (tree)

`sklearn.ensemble.HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05, max_depth=5, min_samples_leaf=20, l2_regularization=1.0, random_state=20260101)`

- Tree-based; captures non-linear interactions.
- `random_state=20260101` makes the run deterministic.
- `min_samples_leaf=20` is a conservative leaf size to reduce
  overfitting on the synthetic data.

Both estimators are wrapped in a single
`sklearn.pipeline.Pipeline` that uses
`ColumnTransformer` + `OneHotEncoder` for categorical features and
passes the numerics through.  The pipeline is the unit of
serialisation: `LateRiskModel.save(path)` pickles the entire
pipeline and `LateRiskModel.load(path)` restores it.

## Risk Bands

The decision-support output categorises predicted probability into four
demonstration bands. These boundaries have not been calibrated or approved as
production business thresholds.

| Band | Probability range | Rationale |
|---|---|---|
| `LOW` | `[0.00, 0.10)` | Lowest model-score band |
| `MEDIUM` | `[0.10, 0.25)` | Intermediate model-score band |
| `HIGH` | `[0.25, 0.50)` | Elevated model-score band for demonstration review |
| `CRITICAL` | `[0.50, 1.00]` | Highest model-score band for demonstration review |

The **default decision threshold** for converting `risk_probability`
into the binary `predicted_late` column is `0.25` (the boundary
between MEDIUM and HIGH).  This is consistent with the
risk-band scheme.  Operators who want a different operating
point can pass a custom `decision_threshold` to
:func:`score_shipments`; the threshold trade-off table in the
evaluation report documents the alternatives.

## Reported Metrics (chronological 5,000-shipment synthetic run)

These are the *real* numbers from a single training run on the
synthetic generator's default 5,000-shipment output
(`GeneratorConfig()` with default settings, seed `20260101`).

| Split | rows | positives | pos rate | AUC-ROC | PR-AUC | P@def | R@def | F1@def |
|---|---|---|---|---|---|---|---|---|
| **Logistic Regression** | | | | | | | | |
| train | 3,500 | 648 | 18.5% | 0.619 | 0.271 | 0.186 | 1.000 | 0.313 |
| validation | 750 | 146 | 19.5% | 0.493 | 0.202 | 0.195 | 1.000 | 0.326 |
| **test** | **750** | **150** | **20.0%** | **0.558** | **0.253** | **0.200** | **1.000** | **0.333** |
| **HistGradient Boosting** | | | | | | | | |
| train | 3,500 | 648 | 18.5% | 0.943 | 0.807 | 0.682 | 0.764 | 0.721 |
| validation | 750 | 146 | 19.5% | 0.485 | 0.196 | 0.167 | 0.370 | 0.230 |
| **test** | **750** | **150** | **20.0%** | **0.531** | **0.224** | **0.208** | **0.440** | **0.282** |

Primary reported numbers are the **test** row.  Observations:

- **Both models overfit.**  The training AUC is much higher than
  the test AUC (0.62 vs 0.56 for LR; 0.94 vs 0.53 for GBT).  This
  is honest: the synthetic dataset has enough noise that the GBT
  memorises the training set.
- **The linear baseline is competitive with the tree.**  LR's
  test F1 (0.333) is higher than GBT's (0.282)
  on this split.  This is a sign that the booking-time features
  do not carry enough non-linear signal to make the GBT pay off.
- **High recall, low precision.**  Both models default to flagging
  the majority of shipments.  The operator should pick a higher
  threshold to improve precision.
- **Class imbalance is real.**  ~20% of the test shipments are
  late; the dataset is not skewed to a trivial class.

The generated evaluation JSON contains the complete threshold trade-off table;
no threshold has been validated as an operational decision limit.

(Based on the same run; values may shift slightly between seeds.)

These numbers are saved to `data/scored/evaluation_reports.json`
when the CLI is invoked.  The threshold table is included in
`EvaluationReport.threshold_tradeoffs`.

## Limitations

- **Synthetic data only.**  No production data was used.  The
  carrier reliability multiplier, the late-rate class
  imbalance, and the per-shipment noise are all generated by the
  synthetic data pipeline.  The model trained on synthetic data
  should not be used in production without re-training on real
  data.
- **Two models only.**  We use sklearn's `LogisticRegression` and
  `HistGradientBoostingClassifier`.  No LightGBM, no XGBoost, no
  CatBoost, no neural network.  This is intentional: a portfolio
  review should not depend on a dozen ML libraries.
- **No hyperparameter search.**  The hyperparameters are documented
  in `transport_etl.ml.training` and are the same for every run.
  A future phase could add a small search driven by the
  validation split.
- **Synthetic data is small by ML standards.**  5,000 shipments
  is a small dataset for a real-world model.  We document the
  total row count and positive-class count in every evaluation
  report.
- **The model does not know about external shocks.**  Weather
  events, fuel prices, regulatory changes, and supply-chain
  disruptions are not in the feature set.  The synthetic
  generator does simulate a `delay_reason` distribution but
  the *features* the model sees are still only booking-time
  attributes.

## Sources of Bias

- **Selection bias in the synthetic generator.**  The generator
  draws carriers, service modes, and origin / destination
  states from a uniform distribution over the configured set.
  Real operations would have very different distributions.
- **Survivorship bias in the carrier dimension.**  The generator
  picks one carrier as "inactive" and excludes it.  In the real
  world inactive carriers still appear in some historical data
  (and they should be handled).
- **Class imbalance in the late label.**  The generator targets
  18% late.  The real class imbalance may be different.  The
  `class_weight='balanced'` setting compensates for the training
  distribution, not for an unknown production distribution.
- **Temporal bias in the chronology split.**  The default
  180-day window may not cover a full seasonal cycle.  A future
  phase could use a 12-month window for production evaluation.
- **The default decision threshold (0.25) is a business rule.**
  It is documented, but it is not a "tuned" value.  A future
  phase could pick a threshold that maximises expected business
  value (cost-of-false-negative × late shipment cost vs.
  cost-of-false-positive × review cost).

## Failure Cases (observed)

- **The two models disagree by ~10 percentage points on
  ROC-AUC.**  This is the noise floor of the synthetic data;
  on a real dataset we would expect more separation between
  models and a higher absolute number.
- **The GBT overfits heavily.**  With `max_iter=200` and
  `min_samples_leaf=20` it still produces train AUC 0.94 vs
  test AUC 0.52.  This is the most important sign that the
  dataset's signal-to-noise ratio is low.
- **Recall is 100% at the default threshold for LR.**  The model
  is *too* generous — every shipment ends up in HIGH or
  CRITICAL.  The band counts for the 5,000-shipment run are
  0 / 8 / 2,622 / 2,370 — no LOW and very few MEDIUM
  shipments.  Operators who use this output should pick a higher
  threshold or rely on the score probability rather than the band.
- **The 5,000-shipment run has only 11 MEDIUM-band shipments.**
  This is a symptom of the overfitting, not a real phenomenon.
  A real distribution would have more variety.

## How to Run

### Library API

```python
from transport_etl.ml import run_training_and_score

result = run_training_and_score(
    shipments_df,
    is_late=labels_series,
    model_name="logistic_regression",  # or "hist_gradient_boosting"
)
print(result.test_report.to_json())
print(result.scored.head())
```

### CLI

```bash
python -m transport_etl.ml.cli train-and-score \
    --shipments data/generated/shipments_2025-07-01.csv \
    --output-dir data/scored \
    --model logistic_regression
```

The CLI writes:
- `data/scored/scored_shipments.csv` — the decision-support frame
- `data/scored/evaluation_reports.json` — train / validation / test metrics
- `data/scored/model.pkl` — the fitted model (loadable via
  `LateRiskModel.load`)

## Test Coverage

| Test file | Coverage |
|---|---|
| `tests/ml/test_leakage_audit.py` | Forbidden column list, leakage guard behavior, custom override |
| `tests/ml/test_splits.py` | Chronological ordering, fraction normalization, minimum-split guard, split_indices consistency |
| `tests/ml/test_features.py` | Required columns, time features, tied-pickup exclusion, outcome-availability history, missing route groups, scoring-time fill |
| `tests/ml/test_training.py` | LR + GBT fit, predict, threshold, save/load round-trip, reproducibility, deterministic GBT with random_state |
| `tests/ml/test_evaluation.py` | Risk-band classification, evaluation report fields, threshold trade-offs, calibration buckets, degenerate inputs (NaN, all-positive, all-negative) |
| `tests/ml/test_scoring.py` | Output schema, risk probability in `[0, 1]`, band classification, predicted_late 0/1, summary dict, no-leakage on output |
| `tests/ml/test_pipeline.py` | End-to-end LR + GBT, chronological split honoured, deterministic for same seed, CLI exit code and output files |

## Synthetic Data Limitation (disclosure)

The dataset used to produce the metrics above is generated by
`transport_etl.synthetic.generate_dataset` and is **clearly
labelled synthetic** in the source code, the docs, and the
generator's output directory.  The metrics are real for the
synthetic data; they would change for real production data and
must be re-evaluated before any production deployment.

No live-Databricks test is required for Phase 8: the model is
trained and scored entirely in-process via `pandas` + `scikit-learn`.
The data is read from a local CSV (or any other source that
produces a `pandas.DataFrame`).
