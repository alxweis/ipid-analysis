"""Paper heatmap of merged IP-ID strategies by operating system."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb
from loguru import logger
import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

from ipid_analysis.config import (  # noqa: E402
    FIGURES_DIR,
    PROCESSED_DATA_DIR,
    RAW_DATA_DIR,
)
from ipid_analysis.paper_figures import configure_paper_style  # noqa: E402
from ipid_analysis.strategies import (  # noqa: E402
    DEFAULT_MANIFEST,
    STRATEGY_NAMES,
    STRATEGY_PRETTY,
)
from ipid_analysis.strategy_merge import (  # noqa: E402
    StrategyMerge,
    load_manifest,
    resolve_strategy_merge,
)

app = typer.Typer()

KIND = "operating-system-by-strategy"
OS_INPUT_NAME = "os.pq"
GENERAL_PURPOSE_GROUP = "General-Purpose OS"
NETWORK_GROUP = "Network OS"
SUPPORTED_PROTOCOLS = ("icmp", "tcp", "udp-dns")
HEATMAP_STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_CONNECTION",
    "PER_DESTINATION",
    "PER_BUCKET",
    "MULTI",
    "RANDOM",
    "UNCLASSIFIED",
)

# OS_NAME values are emitted by ipid-measure/os/fingerprint.go. Keeping the
# mapping explicit makes future classifier additions fail visibly instead of
# silently disappearing from the paper figure.
OS_GROUPS = {
    GENERAL_PURPOSE_GROUP: (
        ("ubuntu", "Ubuntu"),
        ("debian", "Debian"),
        ("raspbian", "Raspbian"),
        ("rhel", "RHEL / CentOS"),
        ("fedora", "Fedora"),
        ("rocky", "Rocky Linux"),
        ("alma", "AlmaLinux"),
        ("amazon-linux", "Amazon Linux"),
        ("oracle-linux", "Oracle Linux"),
        ("suse", "SUSE"),
        ("alpine", "Alpine Linux"),
        ("arch", "Arch Linux"),
        ("gentoo", "Gentoo"),
        ("linux", "Linux"),
        ("freebsd", "FreeBSD"),
        ("openbsd", "OpenBSD"),
        ("netbsd", "NetBSD"),
        ("bsd", "BSD"),
        ("windows", "Microsoft Windows"),
        ("macos", "macOS"),
        ("solaris", "Solaris"),
        ("aix", "AIX"),
        ("hpux", "HP-UX"),
        ("unix", "Unix"),
    ),
    NETWORK_GROUP: (
        ("cisco-ios", "Cisco IOS"),
        ("cisco-iosxe", "Cisco IOS XE"),
        ("cisco-iosxr", "Cisco IOS XR"),
        ("cisco-nxos", "Cisco NX-OS"),
        ("cisco-asa", "Cisco ASA"),
        ("huawei-vrp", "Huawei VRP"),
        ("juniper-junos", "Juniper Junos"),
        ("mikrotik-routeros", "MikroTik RouterOS"),
        ("fortinet-fortios", "Fortinet FortiOS"),
        ("paloalto-panos", "Palo Alto PAN-OS"),
        ("vyos", "VyOS"),
        ("pfsense", "pfSense"),
        ("opnsense", "OPNsense"),
        ("arista-eos", "Arista EOS"),
        ("extreme-exos", "ExtremeXOS"),
        ("hp-comware", "HP Comware"),
        ("f5-bigip", "F5 BIG-IP"),
        ("checkpoint-gaia", "Check Point Gaia"),
        ("zte", "ZTE"),
        ("openwrt", "OpenWrt"),
        ("dd-wrt", "DD-WRT"),
        ("synology", "Synology"),
        ("qnap", "QNAP"),
        ("truenas", "TrueNAS"),
        ("embedded", "Embedded OS"),
        ("router", "Router OS"),
        ("printer", "Printer OS"),
    ),
}

OS_INFO = {
    os_name: (group, label)
    for group, definitions in OS_GROUPS.items()
    for os_name, label in definitions
}

OUTPUT_SCHEMA = pa.schema(
    [
        ("OS_GROUP", pa.string()),
        ("OS_NAME", pa.string()),
        ("OS_LABEL", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("COUNT", pa.int64()),
        ("OS_TOTAL", pa.int64()),
        ("PERCENTAGE", pa.float64()),
    ]
)

PERCENTAGE_CMAP = LinearSegmentedColormap.from_list(
    "percentage_blues",
    ("#FFFFFF", "#DEEBF7", "#9ECAE1", "#4292C6", "#08519C", "#08306B"),
)


def resolve_os_measurement_id(manifest: dict, protocol: str) -> str | None:
    """Return a protocol's OS measurement id, or None when it is absent."""
    section = manifest.get(protocol)
    if not isinstance(section, dict) or "os" not in section:
        return None
    measurement_id = section["os"]
    if not isinstance(measurement_id, str) or not measurement_id.strip():
        raise ValueError(f"{protocol}.os: expected a non-empty measurement id")
    return measurement_id


def _write_table(table: pa.Table, output_path: Path, compression: str | None) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    pq.write_table(table, temporary, compression=compression)
    temporary.replace(output_path)
    return output_path


def _write_json(output_path: Path, value: dict) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(output_path)
    return output_path


def _save_figure(fig, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        bbox_inches="tight",
        pad_inches=0.02,
        metadata={
            "Title": "Operating system by IP-ID selection strategy",
            "Subject": "Row-normalized merged TCP strategy distributions by OS",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)
    return output_path


def aggregate_os_strategies(
    merged_path: Path,
    os_path: Path,
    output_path: Path,
    *,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict:
    """Join OS fingerprints to merged strategies and normalize by OS."""
    for path in (merged_path, os_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    con = duckdb.connect(config={"threads": threads} if threads else {})
    try:
        population = con.execute(
            """
            WITH strategies AS (
                SELECT IP_ADDR,
                       upper(trim(CAST(IPID_SELECTION_STRATEGY AS VARCHAR))) AS STRATEGY
                FROM read_parquet($merged)
            ), os AS (
                SELECT IP_ADDR, lower(trim(CAST(OS_NAME AS VARCHAR))) AS OS_NAME
                FROM read_parquet($os)
            )
            SELECT
                (SELECT count(*) FROM strategies),
                (SELECT count(DISTINCT IP_ADDR) FROM strategies),
                (SELECT count(*) FROM os),
                (SELECT count(DISTINCT IP_ADDR) FROM os),
                count(*) FILTER (WHERE strategies.IP_ADDR IS NOT NULL),
                count(*) FILTER (WHERE strategies.IP_ADDR IS NULL)
            FROM os
            LEFT JOIN strategies USING (IP_ADDR)
            """,
            {"merged": str(merged_path), "os": str(os_path)},
        ).fetchone()
        (
            merged_rows,
            merged_ips,
            os_rows,
            os_ips,
            matched_rows,
            unmatched_os_rows,
        ) = map(int, population)

        rows = con.execute(
            """
            SELECT lower(trim(CAST(o.OS_NAME AS VARCHAR))) AS OS_NAME,
                   upper(trim(CAST(s.IPID_SELECTION_STRATEGY AS VARCHAR))) AS STRATEGY,
                   count(*)::BIGINT AS N
            FROM read_parquet($os) AS o
            INNER JOIN read_parquet($merged) AS s USING (IP_ADDR)
            GROUP BY 1, 2
            """,
            {"merged": str(merged_path), "os": str(os_path)},
        ).fetchall()
    finally:
        con.close()

    if merged_rows == 0:
        raise ValueError(f"{merged_path}: merged strategy result is empty")
    if os_rows == 0:
        raise ValueError(f"{os_path}: OS result is empty")
    if merged_rows != merged_ips:
        raise ValueError(f"{merged_path}: duplicate IP addresses in merged strategy result")
    if os_rows != os_ips:
        raise ValueError(f"{os_path}: duplicate IP addresses in OS result")
    if matched_rows == 0:
        raise ValueError(f"{os_path}: no OS fingerprints match the merged strategy population")

    known_os = set(OS_INFO)
    unknown_os = sorted({str(os_name) for os_name, _, _ in rows} - known_os)
    if unknown_os:
        raise ValueError(f"unknown OS_NAME values in OS input: {unknown_os}")
    known_strategies = set(STRATEGY_NAMES)
    unknown_strategies = sorted({str(strategy) for _, strategy, _ in rows} - known_strategies)
    if unknown_strategies:
        raise ValueError(f"unknown IP-ID strategies in OS input: {unknown_strategies}")

    counts = {(str(os_name), str(strategy)): int(count) for os_name, strategy, count in rows}
    os_totals = {
        os_name: sum(counts.get((os_name, strategy), 0) for strategy in HEATMAP_STRATEGIES)
        for os_name in OS_INFO
    }
    represented_os = [
        os_name
        for definitions in OS_GROUPS.values()
        for os_name, _ in definitions
        if os_totals[os_name] > 0
    ]
    represented_groups = {OS_INFO[os_name][0] for os_name in represented_os}
    missing_groups = [group for group in OS_GROUPS if group not in represented_groups]
    if missing_groups:
        raise ValueError(
            "OS strategy heatmap requires observations in both OS groups; missing: "
            + ", ".join(missing_groups)
        )

    output_rows = []
    os_summary = {}
    for os_name in represented_os:
        group, label = OS_INFO[os_name]
        total = os_totals[os_name]
        os_summary[os_name] = {
            "group": group,
            "label": label,
            "ip_count": total,
        }
        for strategy in HEATMAP_STRATEGIES:
            count = counts.get((os_name, strategy), 0)
            output_rows.append(
                {
                    "OS_GROUP": group,
                    "OS_NAME": os_name,
                    "OS_LABEL": label,
                    "IPID_SELECTION_STRATEGY": strategy,
                    "COUNT": count,
                    "OS_TOTAL": total,
                    "PERCENTAGE": 100.0 * count / total,
                }
            )

    _write_table(pa.Table.from_pylist(output_rows, schema=OUTPUT_SCHEMA), output_path, compression)
    included_rows = sum(os_totals.values())
    return {
        "merged_ip_count": merged_ips,
        "os_ip_count": os_ips,
        "matched_ip_count": matched_rows,
        "included_ip_count": included_rows,
        "excluded_not_enough_samples_ip_count": matched_rows - included_rows,
        "unmatched_os_ip_count": unmatched_os_rows,
        "os_count": len(represented_os),
        "operating_systems": os_summary,
    }


def _format_ip_count(count: int) -> str:
    if count >= 1_000_000:
        value = f"{count / 1_000_000:.1f}".rstrip("0").rstrip(".")
        return f"{value}M"
    if count >= 100_000:
        return f"{count / 1_000:.0f}k"
    if count >= 1_000:
        value = f"{count / 1_000:.1f}".rstrip("0").rstrip(".")
        return f"{value}k"
    return str(count)


def plot_os_by_strategy(aggregate_path: Path, output_path: Path) -> Path:
    """Render two row-normalized OS-group heatmaps with a shared colorbar."""
    rows = pq.read_table(aggregate_path).to_pylist()
    if not rows:
        raise ValueError(f"{aggregate_path}: no OS strategy rows to plot")

    values = {
        (row["OS_NAME"], row["IPID_SELECTION_STRATEGY"]): float(row["PERCENTAGE"]) for row in rows
    }
    totals = {row["OS_NAME"]: int(row["OS_TOTAL"]) for row in rows}
    group_rows = {
        group: [os_name for os_name, _ in definitions if os_name in totals]
        for group, definitions in OS_GROUPS.items()
    }
    missing_groups = [group for group, os_names in group_rows.items() if not os_names]
    if missing_groups:
        raise ValueError(
            "OS strategy heatmap requires observations in both OS groups; missing: "
            + ", ".join(missing_groups)
        )

    total_os_rows = sum(len(os_names) for os_names in group_rows.values())
    figure_height = max(3.8, 0.30 * total_os_rows + 1.9)
    configure_paper_style()
    fig, axes = plt.subplots(
        nrows=2,
        sharex=True,
        figsize=(7.16, figure_height),
        gridspec_kw={
            "height_ratios": [len(group_rows[group]) for group in OS_GROUPS],
            "hspace": 0.31,
        },
    )

    image = None
    for axis_index, (ax, group) in enumerate(zip(axes, OS_GROUPS, strict=True)):
        os_names = group_rows[group]
        matrix = np.asarray(
            [
                [values.get((os_name, strategy), 0.0) for strategy in HEATMAP_STRATEGIES]
                for os_name in os_names
            ],
            dtype=float,
        )
        image = ax.imshow(
            matrix,
            cmap=PERCENTAGE_CMAP,
            vmin=0,
            vmax=100,
            aspect="auto",
            interpolation="nearest",
        )
        labels = [
            f"{OS_INFO[os_name][1]} ({_format_ip_count(totals[os_name])})" for os_name in os_names
        ]
        ax.set_yticks(np.arange(len(os_names)), labels)
        ax.set_title(group, pad=6)
        ax.tick_params(axis="x", bottom=axis_index == 1, labelbottom=axis_index == 1)
        for row_index in range(matrix.shape[0]):
            for column_index in range(matrix.shape[1]):
                percentage = matrix[row_index, column_index]
                ax.text(
                    column_index,
                    row_index,
                    "-" if percentage == 0 else f"{percentage:.1f}",
                    ha="center",
                    va="center",
                    color="white" if percentage >= 50 else "#222222",
                    fontsize=6.6,
                )

    strategy_labels = [STRATEGY_PRETTY[strategy] for strategy in HEATMAP_STRATEGIES]
    axes[-1].set_xticks(
        np.arange(len(strategy_labels)),
        strategy_labels,
        rotation=35,
        ha="right",
        rotation_mode="anchor",
    )
    axes[-1].set_xlabel("IP-ID Selection Strategy")
    fig.supylabel("Operating System (#IP Addr.)", x=0.018)
    fig.subplots_adjust(left=0.31, right=0.86, bottom=0.25, top=0.94)
    assert image is not None
    colorbar_axis = fig.add_axes([0.885, 0.30, 0.018, 0.48])
    colorbar = fig.colorbar(image, cax=colorbar_axis, ticks=np.arange(0, 101, 20))
    colorbar.set_label("Percentage [%]")
    return _save_figure(fig, output_path)


def render(
    merge: StrategyMerge,
    os_measurement_id: str,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    raw_root: Path = RAW_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> tuple[Path, Path, Path]:
    """Create aggregate Parquet, paper PDF, and metadata JSON."""
    if (
        merge.protocol not in SUPPORTED_PROTOCOLS
        or merge.connection_mode != "no-connection"
        or merge.base.interval != "rt-based"
        or merge.mass.interval != "fixed-interval"
    ):
        raise ValueError(
            "OS strategy heatmap requires an ICMP, TCP, or UDP-DNS "
            "no-connection RT-based base measurement followed by its "
            "fixed-interval mass measurement"
        )
    if not os_measurement_id.strip():
        raise ValueError("OS measurement id must not be empty")

    merged_path = merge.artifact_path(processed_root, "strategies")
    os_path = raw_root / "os" / os_measurement_id / OS_INPUT_NAME
    aggregate_path = merge.artifact_path(processed_root, KIND)
    pdf_path = merge.artifact_path(figures_root, KIND, "pdf")
    json_path = merge.artifact_path(figures_root, KIND, "json")

    stats = aggregate_os_strategies(
        merged_path,
        os_path,
        aggregate_path,
        compression=compression,
        threads=threads,
    )
    plot_os_by_strategy(aggregate_path, pdf_path)
    _write_json(
        json_path,
        {
            "target": merge.target,
            "protocol": merge.protocol,
            "zmap_id": merge.zmap_id,
            "os_measurement_id": os_measurement_id,
            "measurements": {
                "rt_based_base": merge.base.measurement_id,
                "fixed_interval_mass": merge.mass.measurement_id,
            },
            "sources": {
                "merged_strategies": str(merged_path),
                "os": str(os_path),
            },
            "aggregate": str(aggregate_path),
            "figure": KIND,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "methodology": {
                "strategy_input": "merged RT-based base and fixed-interval mass classification",
                "os_input": "ipid-measure OS_NAME joined by IP_ADDR",
                "normalization": "each operating-system row is normalized independently to 100%",
                "excluded_category": "NOT_ENOUGH_SAMPLES is not an IP-ID selection strategy",
                "zero_cell_label": "-",
                "percentage_decimals": 1,
            },
            **stats,
        },
    )
    return pdf_path, json_path, aggregate_path


@app.command()
def main(
    base_target: str = typer.Argument(..., help="TCP RT-based base measurement target"),
    mass_target: str = typer.Argument(..., help="TCP fixed-interval mass measurement target"),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, min=0, help="DuckDB threads; 0 uses all cores"),
) -> None:
    try:
        manifest_data = load_manifest(manifest)
        merge = resolve_strategy_merge(manifest_data, base_target, mass_target)
        os_measurement_id = resolve_os_measurement_id(manifest_data, merge.protocol)
        if os_measurement_id is None:
            raise ValueError(f"{merge.protocol}.os: not present in manifest")
        outputs = render(
            merge,
            os_measurement_id,
            compression=None if compression == "none" else compression,
            threads=threads,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc
    logger.success(f"[{merge.target}] OS strategy heatmap -> {outputs[0]}")


if __name__ == "__main__":
    app()
