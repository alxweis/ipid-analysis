# RANDOM classifier version 8

The 4x25 mass classifier uses `S = min(p_inc, p_gap)` after the existing
deterministic, CONSTANT and MULTI rules. Base classification is unchanged.

## Increment component

Compute `(next - current) mod 65536` in seven fixed views: the full sequence,
the two source-address subsequences, and the four connection subsequences.
Only adjacent positions that both have a reply form a transition in a view;
missing slots are not removed or bridged.

For each view with `m` valid transitions, use exactly one Pearson test with
`k = min(16, floor(m / 5))` bins across the entire 16-bit increment range.
Skip the view if `k < 3` (fewer than 15 transitions). Expected counts use the
exact discrete-uniform bin widths, and p-values use the theoretical chi-square
distribution with `k - 1` degrees of freedom. The five-per-bin rule is a target
expected frequency, not a constraint on observed bin counts.

`p_inc` is the minimum of the available view p-values. An unavailable view
contributes the neutral value one; if all views are unavailable, `p_inc = 1`.
There is no multiscale test, Fisher aggregation, or increment Monte Carlo table.

## Gap component

The circular-spacing Cramer-von Mises component is unchanged. It sorts the
received IP-IDs, retains duplicates as zero gaps, and includes the wrap gap.
It compares the transformed spacing distribution against complete simulated
IID discrete-uniform 16-bit configurations of the same observed length.
The existing gap seed, one-million-simulation specification, and lazy cache
are retained. The raw-IPID uniformity diagnostic is not part of the score.

## Provisional decision threshold

The numeric cutoff `7.89999176049605e-05` is retained **only as a provisional,
uncalibrated value**, as requested. The decision remains `S >= cutoff`, with
at least two received values. The old classifier's calibration does not apply
to this score, and no combined false-rejection rate is claimed. Overlapping
view tests are not independent, and theoretical chi-square tails are approximate.

This change does not select a new threshold, run scientific validation, or
generate validation plots. Those steps must be performed separately before
making accuracy claims. Historical experimental modules remain available;
production no longer calls their multiscale/Fisher implementation.

## Calibrating the current score

The existing offline evaluator has a production-only mode that calls
`candidate_random_score_components` directly, using the same one-million-sample
gap tables and fixed seed as production. No historical score variants are run.
The data seed changes calibration/test sequences, not the gap reference.

```bash
# Smaller exploratory run; not the final calibration.
make calibrate-random-classifier ARGS="--preset screening --seed 20261102"

# Calibration and independent evaluation for the fixed method.
make calibrate-random-classifier ARGS="--preset confirmation --seed 20261102"
```

Confirmation generates 250,000 independent IID uniform 16-bit calibration
sequences per condition. The four conditions are ideal, 20% loss, 20% reordering,
and their combination. Loss removes 20 of 100 positions; reordering in the
combined condition acts on the remaining positions. This covers 100 and 80
replies under those specific impairment models, not arbitrary loss patterns or
every possible reply count.

For each target false-rejection rate, the evaluator sorts the complete scores
in each condition and selects the value at zero-based index `floor(target * N)`.
It takes the smallest condition-specific threshold as the proposed common
threshold. Decisions use `S >= threshold`, so ties at the threshold are accepted.
This limits the strict lower-tail fraction on the calibration data; independent
evaluation is needed to assess the error rate beyond that sample.

The primary target remains 0.05%; 0.01%, 0.025%, and 0.1% are also reported.
Confirmation separately evaluates 100,000 sequences per generator and condition
for each of the paper and held-out generator profiles, using independent random
streams. The proposed thresholds are frozen before those tests. Reports include
per-condition RANDOM false-rejection rates, structured false-RANDOM rates, and
95% Wilson intervals. These are score-compatibility results **before** the
deterministic/CONSTANT/MULTI rules, not complete-classifier confusion matrices.

Outputs are kept separate from historical experiments:

- Data/reports: `data/processed/classifier-validation/random-classifier-view-evaluation/production/confirmation/`
- Figures: `reports/figures/classifier-validation/random-classifier-view-evaluation/production/confirmation/`

Replace `confirmation` with `screening` for the smaller preset. Important files:

- `summary.json`: proposed thresholds under `calibration`, calibration rates by
  condition, score version, sample sizes, and seeds.
- `operating-point-by-scenario.csv`: independent error counts, rates, and confidence
  intervals for each target, generator, profile, and condition.
- `operating-point-results.csv`: aggregate independent results for each target.
- `view-scores.pq`: individual evaluation scores and both component values in
  float64, with condition, profile, generator, sample ID, and reply count.
- `random-classifier-view-review-bundle.zip`: summaries, CSV reports, log, and
  figures for review; the potentially large score Parquet is kept separately.

The command only proposes thresholds. It does not change the production cutoff,
saved classifications, or source files. Review the independent results before
adopting a threshold and regenerating the complete-classifier validation plots.

Existing persisted `strategies.pq` labels are reused by default. An explicit
`--reclassify` is needed to replace them with version-8 decisions; subsequent
derived analyses must then be rebuilt as appropriate. New strategy Parquet
metadata records classifier/score versions and the provisional threshold status.
