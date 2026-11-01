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

Existing persisted `strategies.pq` labels are reused by default. An explicit
`--reclassify` is needed to replace them with version-8 decisions; subsequent
derived analyses must then be rebuilt as appropriate. New strategy Parquet
metadata records classifier/score versions and the provisional threshold status.
