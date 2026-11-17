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
THEMES = "campaign refs params classifier artifacts validation results claims stats constant incs probing tcp-transition plots".split()
SITES = {"af", "as", "au", "eu", "na", "sa"}


def key_order(key):
    parts = key.split(".")
    site = parts[1] if len(parts) > 1 and parts[1] in SITES else ""
    metric = (parts[0], *parts[2:]) if site else tuple(parts)
    return THEMES.index(parts[0]) if parts[0] in THEMES else len(THEMES), metric, site


def formatted(raw, unit):
    if raw is None:
        return ""
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
        "% Keys: topic.[VP].[protocol].[no-conn|conn].[phase].metric.",
        "% VPs: af/as/au/eu/na/sa. Blank values are unavailable, not zero.",
        "% success: sufficient replies; rt-pop: original RT population; pct: percent with metric-specific denominator.",
        "% rt-fi: RT/Mass merge; rt-vs-fi-base: paired probing-method comparison.",
    ]
    previous = None
    for key in sorted(values, key=key_order):
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", key):
            raise ValueError(f"invalid paper key: {key}")
        value = values[key]
        text = formatted(value["raw"], value["unit"])
        if value["unit"] == "pdf-path" and text:
            from paper_values.inputs import validate_plot_path

            text = r"\detokenize{" + validate_plot_path(text) + "}"
        else:
            text = escape(text)
        metric = key_order(key)[1]
        if previous is not None and metric != previous:
            lines.append("")
        lines.append(f"\\setval{{{key}}}{{{text}}}")
        previous = metric
    return "\n".join(lines) + "\n"
