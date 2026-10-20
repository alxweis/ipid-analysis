"""Per-target interprotocol outcomes, streamed through classification and reporting."""

from collections import Counter
from contextlib import ExitStack
import json

import matplotlib.pyplot as plt
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml

from ipid_analysis.paper_figures import (
    COMPACT_PAPER_PDF_PADDING_INCHES,
    COMPACT_PAPER_STROKE_WIDTH,
    configure_compact_validation_style,
)
from ipid_analysis.strategies import (
    CLASSIFIER_VERSION,
    MULTI_MAX_CLUSTERS,
    RANDOM_STRUCTURE_SCORE_VERSION,
    STRATEGY_COLORS,
    STRATEGY_NAMES,
    IPIDStrategy,
    MeasurementConfig,
    _cluster_counts_mass,
    classify_batch,
    classify_batch_mass,
)

STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "PER_DESTINATION",
    "SINGLE",
    "PER_BUCKET",
    "MULTI",
    "RANDOM",
)
COUNTERS = ("PER_DESTINATION", "SINGLE", "PER_BUCKET")
SCHEMA = pa.schema(
    [
        ("IP_ADDR", pa.string()),
        ("PROTOCOL_GROUP", pa.string()),
        ("BASELINE_STRATEGY", pa.string()),
        ("SAMPLE", pa.string()),
        ("OUTCOME", pa.string()),
        ("PROTOCOLS", pa.list_(pa.string())),
        ("OBSERVED_STRATEGIES", pa.list_(pa.string())),
        ("RECEIVED_SAMPLES", pa.list_(pa.int32())),
        ("EXPECTED_SAMPLES", pa.int32()),
        ("RAW_CHANGED_PROTOCOLS", pa.int32()),
        ("STATE_COUNTS", pa.list_(pa.int32())),
        ("SHARED_PAIRS", pa.list_(pa.string())),
        ("COMPATIBLE_PARTITIONS", pa.list_(pa.string())),
        ("PARAMETERS", pa.list_(pa.int32())),
        ("MATCHING_PARAMETER_PAIRS", pa.list_(pa.string())),
        ("COMBINED_MATCH", pa.bool_()),
    ],
    metadata={
        b"interprotocol_outcome_version": b"2",
        b"strategy_classifier_version": CLASSIFIER_VERSION.encode(),
        b"random_structure_score_version": RANDOM_STRUCTURE_SCORE_VERSION.encode(),
    },
)


def prerequisites(cfg, snapshot):
    n = cfg.connection_count * cfg.requests_per_connection
    if cfg.connection_count != 4 or cfg.requests_per_connection not in (4, 25):
        raise ValueError("interprotocol requires 4x4 BASE or 4x25 MASS per protocol")
    section = snapshot.get("interprotocol", {})
    sample = section.get("sample", "base" if n == 16 else "mass")
    if sample not in ("base", "mass") or n != {"base": 16, "mass": 100}[sample]:
        raise ValueError("interprotocol sample and layout disagree")
    if "sample" in section:
        expected_mode = {"base": "rt-based", "mass": "fixed-interval"}[sample]
        if snapshot.get("measurement_mode") != expected_mode:
            raise ValueError("interprotocol sample and measurement_mode disagree")
    indexing = section.get("request_ipid_index", "global")
    if "sample" in section and indexing != "logical":
        raise ValueError("new interprotocol runs require logical request IPID indexing")
    if (
        sample == "mass"
        and snapshot.get("fixed_interval", {}).get("minimum_reply_rate", 0.8) != 0.8
    ):
        raise ValueError("interprotocol MASS requires minimum_reply_rate 0.8")
    if indexing not in ("logical", "global"):
        raise ValueError("unsupported request_ipid_index")
    raw = snapshot.get("request_ip_ids")
    if (
        not isinstance(raw, list)
        or not raw
        or any(type(v) is not int or not 0 <= v < 65536 for v in raw)
    ):
        raise ValueError("request_ip_ids must be a nonempty list of 16-bit integers")
    pattern = np.asarray(raw)
    p = len(cfg.protocols)
    positions = np.arange(n)
    patterns = [
        pattern[(positions if indexing == "logical" else positions * p + j) % len(pattern)]
        for j in range(p)
    ]
    if any(np.unique(x).size < 2 for x in patterns):
        raise ValueError("request_ip_ids must vary within every protocol sequence")
    return sample, patterns


def classify_rows(rows, cfg, snapshot, *, random_null_tables=None):
    from ipid_analysis.interprotocol import _increments_ok, compatible

    sample, patterns = prerequisites(cfg, snapshot)
    n = cfg.connection_count * cfg.requests_per_connection
    p = len(cfg.protocols)
    values = np.full((len(rows), n * p), -1, dtype=np.int64)
    for i, row in enumerate(rows):
        baseline = row["IPID_SELECTION_STRATEGY"]
        if baseline not in STRATEGIES:
            raise ValueError(f"unsupported baseline {baseline}")
        if "sample" in snapshot.get("interprotocol", {}) and (
            (baseline in ("MULTI", "RANDOM")) != (sample == "mass")
        ):
            raise ValueError("target strategy is in the wrong BASE/MASS population")
        seq = [int(x) if x != "-" else -1 for x in row["IPID_SEQUENCE"].split(",")]
        if len(seq) != n * p or any(x < -1 or x >= 65536 for x in seq):
            raise ValueError("malformed interprotocol sequence")
        values[i] = seq
    labels = np.full((len(rows), p), int(IPIDStrategy.NOT_ENOUGH_SAMPLES), dtype=np.int8)
    received = np.zeros((len(rows), p), dtype=int)
    required = 16 if sample == "base" else 80
    for j in range(p):
        sub = values[:, j::p]
        received[:, j] = (sub >= 0).sum(axis=1)
        enough = received[:, j] >= required
        config = MeasurementConfig(4, cfg.requests_per_connection, patterns[j])
        if enough.any():
            if sample == "base":
                labels[enough, j] = classify_batch(sub[enough].astype(np.uint16), config)
            else:
                array = pa.array(
                    [[int(v) for v in seq] for seq in sub[enough]], type=pa.list_(pa.int64())
                )
                labels[enough, j] = classify_batch_mass(
                    array, config, random_null_tables=random_null_tables
                )
    output = []
    for i, row in enumerate(rows):
        baseline = row["IPID_SELECTION_STRATEGY"]
        observed = [STRATEGY_NAMES[int(code)] for code in labels[i]]
        enough = bool((received[i] >= required).all())
        raw_changes = sum(label != baseline for label in observed) if enough else None
        outcome = "NOT_ENOUGH_SAMPLES"
        pairs, partitions, parameters, states = [], [], [], None
        combined = None
        matching_parameters = []
        sequence = values[i]
        if enough and baseline in COUNTERS:
            # Counter populations use complete BASE sequences in new campaigns.
            if (sequence < 0).any():
                raise ValueError("counter sharing requires complete sequences")
            if compatible(sequence, cfg, baseline, cfg.protocols):
                outcome, states = "PROTOCOL_SHARED", [p, 0, 0]
            else:
                for a in range(p):
                    for b in range(a + 1, p):
                        pair = (cfg.protocols[a], cfg.protocols[b])
                        if compatible(sequence, cfg, baseline, pair):
                            name = "-".join(pair)
                            pairs.append(name)
                            if all(observed[j] == baseline for j in range(p) if j not in (a, b)):
                                partitions.append(name)
                if p == 3 and partitions:
                    outcome, states = "PARTIAL_PROTOCOL_SHARED", [2, 1, 0]
                elif raw_changes == 0:
                    outcome, states = "PROTOCOL_ISOLATED", [0, p, 0]
                else:
                    outcome = "STRATEGY_CHANGED"
                    states = [2, 0, 1] if p == 3 and pairs else [0, p - raw_changes, raw_changes]
        elif enough:
            if raw_changes:
                outcome = "STRATEGY_CHANGED"
            elif baseline in ("REFLECTION", "CONSTANT"):
                for j in range(p):
                    seq = sequence[j::p]
                    valid = seq >= 0
                    parameter = (
                        ((seq[valid] - patterns[j][valid]) % 65536)[0]
                        if baseline == "REFLECTION"
                        else seq[valid][0]
                    )
                    parameters.append(int(parameter))
                matching_parameters = [
                    "-".join((cfg.protocols[a], cfg.protocols[b]))
                    for a in range(p)
                    for b in range(a + 1, p)
                    if parameters[a] == parameters[b]
                ]
                if len(set(parameters)) == 1:
                    outcome = "SAME_PARAMETER"
                elif matching_parameters:
                    outcome = "PARTIALLY_MATCHING_PARAMETERS"
                else:
                    outcome = "DIFFERENT_PARAMETERS"
            elif baseline == "MULTI":
                # A combined MULTI match uses the established cluster rule after
                # ruling out earlier position-dependent classes with the real layout.
                valid = sequence >= 0
                data = np.where(valid, sequence, 0)[None, :]
                clusters = _cluster_counts_mass(data, valid[None, :], np.array([valid.sum()]))[0]
                earlier = len(set(sequence[valid])) == 1
                if valid.all():
                    request = np.stack(patterns, axis=1).ravel()
                    earlier |= np.unique((sequence - request) % 65536).size == 1
                    earlier |= any(compatible(sequence, cfg, s, cfg.protocols) for s in COUNTERS)
                    _, connection = cfg.layout()
                    earlier |= all(_increments_ok(sequence[connection == c], 1) for c in range(4))
                combined = bool(not earlier and 1 < clusters <= MULTI_MAX_CLUSTERS)
                outcome = "SAME_INTER_STRATEGY" if combined else "DIFFERENT_INTER_STRATEGY"
            else:
                # Joint 200/300-position RANDOM calibration is explicitly deferred.
                outcome = "UNCHANGED"
        output.append(
            {
                "IP_ADDR": row["IP_ADDR"],
                "PROTOCOL_GROUP": "-".join(cfg.protocols),
                "BASELINE_STRATEGY": baseline,
                "SAMPLE": sample,
                "OUTCOME": outcome,
                "PROTOCOLS": list(cfg.protocols),
                "OBSERVED_STRATEGIES": observed,
                "RECEIVED_SAMPLES": received[i].tolist(),
                "EXPECTED_SAMPLES": n,
                "RAW_CHANGED_PROTOCOLS": raw_changes,
                "STATE_COUNTS": states,
                "SHARED_PAIRS": pairs,
                "COMPATIBLE_PARTITIONS": partitions,
                "PARAMETERS": parameters,
                "MATCHING_PARAMETER_PAIRS": matching_parameters,
                "COMBINED_MATCH": combined,
            }
        )
    return output


class Summary:
    def __init__(self):
        self.counts = Counter()
        self.transitions = Counter()
        self.relationships = Counter()
        self.parameter_relationships = Counter()
        self.rows = 0

    def add(self, rows):
        for row in rows:
            self.rows += 1
            key = (row["BASELINE_STRATEGY"], row["PROTOCOL_GROUP"], row["OUTCOME"])
            self.counts[key] += 1
            if row["MATCHING_PARAMETER_PAIRS"]:
                self.parameter_relationships[(*key, tuple(row["MATCHING_PARAMETER_PAIRS"]))] += 1
            if row["SHARED_PAIRS"]:
                self.relationships[
                    (*key, tuple(row["SHARED_PAIRS"]), tuple(row["COMPATIBLE_PARTITIONS"]))
                ] += 1
            if row["OUTCOME"] == "STRATEGY_CHANGED":
                self.transitions[
                    (*key, tuple(row["OBSERVED_STRATEGIES"]), tuple(row["STATE_COUNTS"] or []))
                ] += 1

    def write(self, path, measurements=None):
        groups = sorted({g for _, g, _ in self.counts} | set(measurements or {}))
        report = {
            "version": 2,
            "rows": self.rows,
            "unit": "IP addresses within each protocol group",
            "classifier_version": CLASSIFIER_VERSION,
            "random_score_version": RANDOM_STRUCTURE_SCORE_VERSION,
            "combined_random_analysis": "deferred",
            "state_count_order": ["shared", "isolated", "changed"],
            "parameter_relationships": [
                {
                    "baseline": s,
                    "protocol_group": g,
                    "outcome": o,
                    "matching_pairs": list(pairs),
                    "count": count,
                }
                for (s, g, o, pairs), count in sorted(self.parameter_relationships.items())
            ],
            "partial_relationships": [
                {
                    "baseline": s,
                    "protocol_group": g,
                    "outcome": o,
                    "shared_pairs": list(pairs),
                    "compatible_partitions": list(partitions),
                    "count": count,
                }
                for (s, g, o, pairs, partitions), count in sorted(self.relationships.items())
            ],
            "measurements": measurements or {},
            "interpretation": "Shared means model-compatible; Changed means an observed label change including UNCLASSIFIED",
            "groups": {
                g: {
                    "rows": sum(v for (s, group, o), v in self.counts.items() if group == g),
                    "counts": {
                        f"{s}:{o}": v
                        for (s, group, o), v in sorted(self.counts.items())
                        if group == g
                    },
                }
                for g in groups
            },
        }
        path.write_text(json.dumps(report, indent=2) + "\n")
        for strategy in STRATEGIES:
            entries = []
            for (s, group, outcome, labels, states), count in sorted(self.transitions.items()):
                if s != strategy:
                    continue
                denominator = sum(
                    v for (base, g, _), v in self.counts.items() if base == s and g == group
                )
                entries.append(
                    {
                        "protocol_group": group,
                        "observed": dict(zip(group.split("-"), labels)),
                        "state_counts": list(states) or None,
                        "raw_changed_protocols": sum(x != s for x in labels),
                        "count": count,
                        "percent_of_baseline_group": 100 * count / denominator,
                    }
                )
            name = (
                "interprotocol-campaign-"
                + strategy.lower().replace("_", "-")
                + "-alternatives.json"
            )
            (path.parent / name).write_text(
                json.dumps(
                    {
                        "baseline_strategy": strategy,
                        "unit": "IP addresses",
                        "selection": "STRATEGY_CHANGED after sharing precedence",
                        "alternatives": entries,
                    },
                    indent=2,
                )
                + "\n"
            )


def classify_file(raw_path, snapshot_path, output_dir, *, figure_dir=None):
    from ipid_analysis.interprotocol import load_interprotocol_config

    cfg = load_interprotocol_config(snapshot_path)
    snapshot = yaml.safe_load(snapshot_path.read_text())
    prerequisites(cfg, snapshot)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "interprotocol-outcomes.pq"
    temporary = path.with_suffix(".pq.part")
    summary = Summary()
    with (
        pq.ParquetWriter(temporary, SCHEMA, compression="zstd") as writer,
        pq.ParquetFile(raw_path) as parquet,
    ):
        for batch in parquet.iter_batches(
            batch_size=2048, columns=["IP_ADDR", "IPID_SELECTION_STRATEGY", "IPID_SEQUENCE"]
        ):
            rows = classify_rows(batch.to_pylist(), cfg, snapshot)
            writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA))
            summary.add(rows)
    temporary.replace(path)
    summary.write(output_dir / "interprotocol-summary.json")
    if figure_dir is not None:
        plot_outcomes(summary, figure_dir)
    return path


def plot_outcomes(summary, figure_dir):
    from ipid_analysis.interprotocol import GROUPS

    configure_compact_validation_style()
    figure_dir.mkdir(parents=True, exist_ok=True)
    for strategy in STRATEGIES:
        if strategy in COUNTERS:
            categories = [
                ("PROTOCOL_SHARED", "Protocol-Shared"),
                ("PARTIAL_PROTOCOL_SHARED", "Partial Protocol-Shared"),
                ("PROTOCOL_ISOLATED", "Protocol-Isolated"),
            ]
        elif strategy in ("REFLECTION", "CONSTANT"):
            noun = "Offset" if strategy == "REFLECTION" else "Value"
            categories = [
                ("SAME_PARAMETER", "Same " + noun),
                ("PARTIALLY_MATCHING_PARAMETERS", "Partially Matching " + noun + "s"),
                ("DIFFERENT_PARAMETERS", "Different " + noun + "s"),
            ]
        elif strategy == "MULTI":
            categories = [
                ("SAME_INTER_STRATEGY", "Same Inter-Strategy"),
                ("DIFFERENT_INTER_STRATEGY", "Different Inter-Strategy"),
            ]
        else:
            categories = [("UNCHANGED", "0 Strategies Changed")]
        categories += [
            ("STRATEGY_CHANGED", "≥1 Strategy Changed"),
            ("NOT_ENOUGH_SAMPLES", "Not Enough Samples"),
        ]
        color_names = (
            ["PER_BUCKET", "PER_DESTINATION", "RANDOM", "SINGLE", "NOT_ENOUGH_SAMPLES"]
            if strategy in (*COUNTERS, "REFLECTION", "CONSTANT")
            else ["PER_BUCKET", "RANDOM", "SINGLE", "NOT_ENOUGH_SAMPLES"]
        )
        if strategy == "RANDOM":
            color_names = ["PER_BUCKET", "SINGLE", "NOT_ENOUGH_SAMPLES"]
        colors = [STRATEGY_COLORS[name] for name in color_names]
        fig, ax = plt.subplots(figsize=(6.5, 2.55))
        x = np.arange(len(GROUPS)) * 0.84
        totals = [sum(summary.counts[strategy, g, o] for o, _ in categories) for g in GROUPS]
        bottom = np.zeros(len(GROUPS))
        for (outcome, label), color in zip(categories, colors, strict=True):
            heights = [
                100 * summary.counts[strategy, g, outcome] / n if n else 0
                for g, n in zip(GROUPS, totals)
            ]
            ax.bar(
                x,
                heights,
                bottom=bottom,
                width=0.48,
                color=color,
                label=label,
                edgecolor="white",
                linewidth=COMPACT_PAPER_STROKE_WIDTH,
            )
            bottom += heights
        ax.set_xticks(
            x,
            [r"$\cap$".join(g.upper().split("-")) + f"\n(N={n})" for g, n in zip(GROUPS, totals)],
        )
        ax.set_xlim(x[0] - 0.34, x[-1] + 0.34)
        ax.set_ylim(0, 100)
        ax.set_yticks(np.arange(0, 101, 20))
        ax.set_ylabel("Percentage [%]")
        ax.set_xlabel("Protocol Intersection (#IP Addr.)")
        ax.set_axisbelow(True)
        ax.grid(axis="y", color="#D9D9D9", linewidth=COMPACT_PAPER_STROKE_WIDTH)
        ax.legend(
            frameon=False,
            loc="lower center",
            bbox_to_anchor=(0.5, 1.01),
            ncol=3 if len(categories) == 5 else 2,
            fontsize=7,
        )
        fig.subplots_adjust(left=0.095, right=0.995, bottom=0.28, top=0.73)
        fig.savefig(
            figure_dir / ("interprotocol-campaign-" + strategy.lower().replace("_", "-") + ".pdf"),
            bbox_inches="tight",
            pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES,
        )
        plt.close(fig)


def classify_campaign(run_manifest_path, *, raw_root, output_root, figures_root):
    from ipid_analysis.interprotocol import GROUPS, load_run_manifest, measurement_items

    manifest = load_run_manifest(run_manifest_path)
    output = output_root / manifest["campaign_id"] / "runs" / manifest["run_id"]
    figures = figures_root / manifest["campaign_id"] / "runs" / manifest["run_id"]
    output.mkdir(parents=True, exist_ok=True)
    combined = output / "interprotocol-campaign-outcomes.pq"
    summary = Summary()
    measurements = {}
    with pq.ParquetWriter(combined.with_suffix(".pq.part"), SCHEMA, compression="zstd") as writer:
        for group in GROUPS:
            if group not in manifest["groups"]:
                continue
            measurements[group] = dict(measurement_items(manifest["groups"][group]))
            for sample, mid in measurements[group].items():
                folder = raw_root / mid
                snapshot = yaml.safe_load((folder / "interprotocol.snapshot.yaml").read_text())
                if tuple(snapshot.get("interprotocol", {}).get("protocols", [])) != GROUPS[group]:
                    raise ValueError("snapshot protocol group differs from manifest")
                if (
                    manifest["version"] == 2
                    and snapshot.get("interprotocol", {}).get("sample") != sample
                ):
                    raise ValueError("snapshot sample differs from manifest")
                result = classify_file(
                    folder / "interprotocol.pq",
                    folder / "interprotocol.snapshot.yaml",
                    output / group / sample,
                )
                with pq.ParquetFile(result) as parquet:
                    for batch in parquet.iter_batches(batch_size=2048):
                        writer.write_batch(batch)
                        summary.add(batch.to_pylist())
    combined.with_suffix(".pq.part").replace(combined)
    summary.write(output / "interprotocol-campaign-summary.json", measurements)
    # Parquet detail reports stay streamed; no campaign-sized Python dictionaries.
    with ExitStack() as stack:
        writers = {
            s: stack.enter_context(
                pq.ParquetWriter(
                    output
                    / (
                        "interprotocol-campaign-"
                        + s.lower().replace("_", "-")
                        + "-alternatives.pq"
                    ),
                    SCHEMA,
                    compression="zstd",
                )
            )
            for s in STRATEGIES
        }
        with pq.ParquetFile(combined) as parquet:
            for batch in parquet.iter_batches(batch_size=8192):
                changed = pa.Table.from_batches([batch]).filter(
                    pc.equal(batch.column("OUTCOME"), "STRATEGY_CHANGED")
                )
                for strategy, writer in writers.items():
                    writer.write_table(
                        changed.filter(pc.equal(changed["BASELINE_STRATEGY"], strategy))
                    )
    plot_outcomes(summary, figures)
    (output / "interprotocol-campaign-run.json").write_text(
        json.dumps(
            {
                "source_manifest": str(run_manifest_path),
                "measurements": measurements,
                "version": 1,
            },
            indent=2,
        )
        + "\n"
    )
    return output
