"""Small ndpi-compatible writer; ordinary values and exact numbers use getval."""

from decimal import Decimal
import re

PREAMBLE = r"""% Generated paper values; requires todonotes for missing values.
\providecommand{\setval}[2]{\expandafter\gdef\csname texval@#1\endcsname{#2}}
\providecommand{\getval}[1]{%
  \ifcsname texval@#1\endcsname%
    \expandafter\ifx\csname texval@#1\endcsname\empty%
      \todo[inline]{Missing value: \detokenize{#1}}%
    \else\csname texval@#1\endcsname\fi%
  \else\todo[inline]{Missing value: \detokenize{#1}}\fi}
"""
THEMES = "params classifier validation results fig".split()
SITES = {"af", "as", "au", "eu", "na", "sa"}
PHASES = "zmap rt-base fi-mass repeat fi-base rt-fi rt-vs-fi-base fi-to-rt-success".split()
PROTOCOLS = "icmp tcp udp".split()


def pipeline_order(metric):
    """Order protocol families by their workflow, retaining exact keys and VP grouping."""
    ranks = [0] * len(metric)
    if len(metric) > 1 and metric[0] in ("results", "fig"):
        families = (*PROTOCOLS, "reference", "interprotocol", "vp-comparison")
        ranks[1] = families.index(metric[1]) if metric[1] in families else len(families)
        protocol_index = (
            1
            if metric[1] in PROTOCOLS
            else 2
            if metric[1] in ("reference", "vp-comparison")
            and len(metric) > 2
            and metric[2] in PROTOCOLS
            else None
        )
        if protocol_index is not None:
            protocol = metric[protocol_index]
            if protocol_index == 2:
                ranks[2] = PROTOCOLS.index(protocol)
            phase_index = protocol_index + 1
            if protocol == "tcp" and phase_index < len(metric):
                modes = ("zmap", "no-conn", "conn", "rt-no-conn-to-conn")
                mode = metric[phase_index]
                ranks[phase_index] = modes.index(mode) if mode in modes else len(modes)
                phase_index += 1 if mode in ("no-conn", "conn") else 0
            if phase_index < len(metric) and (
                protocol != "tcp" or metric[protocol_index + 1] in ("no-conn", "conn")
            ):
                phase = metric[phase_index]
                ranks[phase_index] = PHASES.index(phase) if phase in PHASES else len(PHASES)
    if "anycast" in metric:
        for i, part in enumerate(metric):
            if (
                i
                and metric[i - 1] in ("addresses", "prefixes", "unlocated")
                and part in ("original", "excluded", "retained")
            ):
                ranks[i] = ("original", "excluded", "retained").index(part)
    return tuple(zip(ranks, metric))


def key_order(key):
    parts = key.split(".")
    site = parts[1] if len(parts) > 1 and parts[1] in SITES else ""
    metric = (parts[0], *parts[2:]) if site else tuple(parts)
    comparison = parts[:-1] if parts[-1:] == ["full"] else parts
    if key.startswith("results.vp-comparison.") and comparison[-1:] == ["mean"]:
        metric = (*comparison[:-1], "full" if comparison != parts else "", "mean")
    elif key.startswith("results.vp-comparison.") and comparison[-2:] in (
        ["min", "pct"],
        ["max", "pct"],
        ["range", "pp"],
    ):
        # Keep each historical or Full min/max/range block contiguous.
        metric = (*comparison[:-2], "full" if comparison != parts else "", *comparison[-2:])
    return (
        THEMES.index(parts[0]) if parts[0] in THEMES else len(THEMES),
        pipeline_order(metric),
        site,
    )


def blank_line_group(key):
    parts = key.split(".")
    metric = (parts[0], *parts[2:]) if len(parts) > 1 and parts[1] in SITES else tuple(parts)
    if "anycast" in metric:
        if "continent" in metric:
            return metric[: metric.index("continent") + 2]
        return metric[: metric.index("anycast") + 1]
    if metric[:2] == ("results", "interprotocol") and len(metric) > 3:
        return metric[:3]  # one strategy's groups, outcomes and detail rows
    if metric[:2] == ("fig", "interprotocol"):
        return metric[:2]
    if "rt-outlier-score" in metric:
        return metric[: metric.index("rt-outlier-score") + 1]
    for prefix in (
        ("results", "tcp", "conn", "rt-base", "constant"),
        ("results", "tcp", "conn", "rt-base", "per-connection", "handshake"),
        ("results", "tcp", "no-conn", "rt-fi", "constant"),
    ):
        if metric[: len(prefix)] == prefix and len(metric) > len(prefix):
            return prefix  # retain adjacent VP lines and both historical/.full keys
    comparison = parts[:-1] if parts[-1:] == ["full"] else parts
    if key.startswith("results.vp-comparison.") and comparison[-1:] == ["mean"]:
        return (*comparison[:-1], "full") if comparison != parts else tuple(parts[:-1])
    if key.startswith("results.vp-comparison.") and comparison[-2:] in (
        ["min", "pct"],
        ["max", "pct"],
        ["range", "pp"],
    ):
        return (*comparison[:-2], "full") if comparison != parts else tuple(parts[:-2])
    if key.startswith("validation."):
        family = parts[1]
        if family == "e2e":
            return tuple(parts[:3])  # one condition or the shared routing block
        if family.startswith("fi-") or family == "lossy-reordered":
            return ("validation", "fi")
        return ("validation", family.removesuffix("-score"))
    if key.startswith("fig.validation."):
        family = "fi" if parts[2].startswith("fi-") else parts[2]
        return ("fig", "validation", family)
    return key_order(key)[1]


def formatted(raw, unit, key=""):
    if raw is None:
        return ""
    if (
        key.startswith("results.vp-comparison.")
        and re.search(r"\.range\.pp(?:\.full)?$", key)
        and unit == "percentage-points"
        and Decimal(str(raw)) < Decimal("0.01")
    ):
        return "<0.01"
    if unit in ("percent", "percentage-points"):
        return f"{Decimal(str(raw)):.2f}" + ("%" if unit == "percent" else "")
    if isinstance(raw, bool):
        return "true" if raw else "false"
    # Keep float precision for the later, explicitly invoked getvalcompact.
    return str(raw)


def escape(text):
    escapes = {
        "\\": r"\textbackslash{}",
        "{": r"\{",
        "}": r"\}",
        "%": r"\%",
        "&": r"\&",
        "#": r"\#",
        "_": r"\_",
        "$": r"\$",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(escapes.get(c, c) for c in text)


def render(values, campaign):
    lines = [
        PREAMBLE.rstrip(),
        f"% Campaign: {campaign}",
        "% Prefixes: results, validation, classifier, params, fig.",
        "% Measurement keys: prefix.VP.protocol.[TCP mode].phase.metric; TCP mode: no-conn|conn.",
        "% VPs: af/as/au/eu/na/sa. Blank values are unavailable, not zero.",
        "% success: sufficient replies; rt-pop: original RT population; pct: percent with metric-specific denominator.",
        "% fi-mass.all: Phase 2 plot population, including insufficient/missing replies.",
        "% rt-fi.success-all.count: RT-based successes with all Phase 2 outcomes; counts each IP once.",
        "% rt-fi: RT/Mass merge; rt-vs-fi-base: paired probing-method comparison.",
        "% Per protocol: ZMap -> RT-BASE -> FI-MASS/repeats -> FI-BASE -> RT/FI merge -> comparisons.",
    ]
    previous = None
    for key in sorted(values, key=key_order):
        if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]*", key):
            raise ValueError(f"invalid paper key: {key}")
        value = values[key]
        text = formatted(value["raw"], value["unit"], key)
        if value["unit"] == "pdf-path" and text:
            from paper_values.inputs import validate_plot_path

            text = r"\detokenize{" + validate_plot_path(text) + "}"
        else:
            text = escape(text)
            if value["unit"] == "" and isinstance(value["raw"], str):
                # Insert TeX spacing only here; recorded values and caches stay raw.
                text = re.sub(
                    r"^([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*(ns|us|µs|μs|ms|s|m|h)$",
                    r"\1~\2",
                    text,
                )
        metric = blank_line_group(key)
        if previous is not None and metric != previous:
            lines.append("")
        lines.append(f"\\setval{{{key}}}{{{text}}}")
        previous = metric
    return "\n".join(lines) + "\n"
