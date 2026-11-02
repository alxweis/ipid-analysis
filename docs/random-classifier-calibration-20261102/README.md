# RANDOM operating-point confirmation, 2026-11-02

The version-9 classifier adopts `tau = 4.0383019016847365e-05` for the unchanged
`increment-pearson-gap-min-v4` score. The decision is `S >= tau`. The primary
target is 0.05% true-RANDOM false rejection, using the minimum condition-specific
threshold from IID uniform 16-bit calibration sequences.

## Recorded run

```bash
make calibrate-random-classifier ARGS="--preset confirmation --seed 20261102"
```

The evaluator specification is recorded in PR #124 (head commit
`0111ebb59afc2874f73842203c19def30a1abd9a`). The supplied analysis-VM review ZIP has SHA-256
`5166532fddb42fb901da17ac0888e589e938075de4413ce34bd4730ecc7cbbbc`.
The following files are preserved byte-for-byte from that ZIP:

- [summary.json](summary.json): complete configuration, seeds, calibration
  thresholds and rates, score version, and reference-table specification.
- [operating-point-by-scenario.csv](operating-point-by-scenario.csv): independent
  per-condition/generator/profile error counts, rates, and 95% Wilson intervals
  for all four target rates.
- [operating-point-results.csv](operating-point-results.csv): aggregate results
  for each target and generator profile.

The original summary records classifier version 8, the then-current provisional
cutoff, and `threshold_updated: false`. Those are historical facts of the run:
the command proposed the new threshold without modifying production. Version 9
adopts that proposal while retaining the same score and gap null reference.

There are 250,000 calibration sequences per condition and 100,000 independent
evaluation sequences per generator, condition, and profile. The paper profile
has eight generators and the held-out profile has sixteen. Calibration streams
101/102 are separate from evaluation streams 201/202. Conditions within a profile
reuse underlying sequences, so their results must not be pooled as independent
observations. The gap reference uses 1,000,000 null samples per reply count,
seed 20260950, and `empirical-discrete-16bit-circular-cvm-v1`.

## Primary independent RANDOM results

Each entry below is the false-rejection percentage among 100,000 sequences.
The intervals are per-cell 95% Wilson intervals, not simultaneous bounds.

| Condition | Paper profile | 95% interval | Held-out profile | 95% interval |
| --- | ---: | --- | ---: | --- |
| Ideal | 0.041% | 0.03023%-0.05561% | 0.037% | 0.02685%-0.05099% |
| 20% random loss | 0.029% | 0.02019%-0.04165% | 0.034% | 0.02433%-0.04751% |
| 20% reordering | 0.041% | 0.03023%-0.05561% | 0.037% | 0.02685%-0.05099% |
| Loss + reordering | 0.040% | 0.02938%-0.05446% | 0.025% | 0.01694%-0.03690% |

All observed point estimates are below the target. Some upper confidence limits
exceed 0.05%; this is an empirically calibrated operating point, not a guarantee
that the population rate is at most 0.05%.

## Scope and limitations

The reference population is IID discrete-uniform 16-bit sequences. Tested
conditions have 100 replies, or 80 replies after random removal of exactly 20
positions. Reordering permutes a selected 20% of received positions. Results do
not establish a bound for arbitrary loss patterns, other reply counts, or the
Internet's distribution of generators.

These reports evaluate score compatibility before the deterministic, CONSTANT,
and MULTI rules. Structured examples remain challenging: in the paper profile,
44.473% of PER_BUCKET sequences pass the score under loss plus reordering; in the
held-out profile, 62.805% of MULTI_COUNTER_16 sequences pass under that condition.
These are not complete-classifier error rates and do not prove a random generator.

Regenerate complete-classifier validation and final figures after adopting the
threshold. Existing saved labels require explicit `--reclassify` to be replaced.
