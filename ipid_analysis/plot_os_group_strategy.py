"""Postprocess canonical OS tags and plot IP-ID strategies by OS group."""

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

from matplotlib.colors import LinearSegmentedColormap
import matplotlib.pyplot as plt

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement, resolve
from ipid_analysis.paper_figures import (
    configure_paper_style,
    linux_libertine_font_properties,
)
from ipid_analysis.strategies import (
    DEFAULT_MANIFEST,
    STRATEGY_NAMES,
    STRATEGY_PRETTY,
)
from ipid_analysis.strategy_merge import (
    StrategyMerge,
    load_manifest,
    resolve_strategy_merge,
)

app = typer.Typer()

KIND = "operating-system-group-by-strategy"
OS_INPUT_NAME = "os.pq"
OS_GROUP_INPUT_NAME = "os-groups.pq"
TAXONOMY_VERSION = "3"
SUPPORTED_PROTOCOLS = ("icmp", "tcp", "udp-dns")
GENERAL_PURPOSE_SECTION = "General-Purpose OS"
NETWORK_SECTION = "Network / Appliance OS"
EMBEDDED_SECTION = "Embedded / RTOS"
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
    "NOT_ENOUGH_SAMPLES",
)

RAW_OS_COLUMNS = (
    "IP_ADDR",
    "OS_STATUS",
    "OS_TAG",
    "SSH_OS_TAG",
    "SMB_OS_TAG",
    "HTTP_OS_TAG",
    "HTTPS_OS_TAG",
    "SNMP_OS_TAG",
    "DNS_OS_TAG",
    "SSH_SERVER_ID",
    "SMB_NATIVE_OS",
    "HTTP_SERVER",
    "HTTPS_SERVER",
    "SNMP_SYS_DESCR",
    "DNS_VERSION_BIND",
)


def _groups(section: str, *definitions: tuple[str, str]) -> dict[str, tuple[str, str]]:
    return {name: (section, label) for name, label in definitions}


GROUP_INFO = {
    **_groups(
        GENERAL_PURPOSE_SECTION,
        ("ubuntu", "Ubuntu"),
        ("debian", "Debian"),
        ("raspbian", "Raspbian"),
        ("rhel", "RHEL"),
        ("centos", "CentOS"),
        ("fedora", "Fedora"),
        ("rocky-linux", "Rocky Linux"),
        ("almalinux", "AlmaLinux"),
        ("amazon-linux", "Amazon Linux"),
        ("oracle-linux", "Oracle Linux"),
        ("kali-linux", "Kali Linux"),
        ("linux-mint", "Linux Mint"),
        ("manjaro", "Manjaro"),
        ("nixos", "NixOS"),
        ("clear-linux", "Clear Linux"),
        ("photon-os", "VMware Photon OS"),
        ("flatcar", "Flatcar Container Linux"),
        ("coreos", "CoreOS"),
        ("slackware", "Slackware"),
        ("suse", "SUSE"),
        ("opensuse", "openSUSE"),
        ("euleros", "EulerOS"),
        ("zorin", "Zorin OS"),
        ("alpine", "Alpine Linux"),
        ("arch-linux", "Arch Linux"),
        ("gentoo", "Gentoo"),
        ("linux", "Linux"),
        ("freebsd", "FreeBSD"),
        ("openbsd", "OpenBSD"),
        ("netbsd", "NetBSD"),
        ("bsd", "Other BSD"),
        ("windows", "Microsoft Windows"),
        ("apple", "Apple (unspecified OS)"),
        ("macos", "macOS"),
        ("apple-ios", "Apple iOS"),
        ("android", "Android"),
        ("chromeos", "ChromeOS"),
        ("solaris", "Solaris"),
        ("aix", "AIX"),
        ("hpux", "HP-UX"),
        ("zos", "IBM z/OS"),
        ("openvms", "OpenVMS"),
        ("vmware-esxi", "VMware ESXi"),
        ("proxmox-ve", "Proxmox VE"),
        ("server", "Generic server"),
    ),
    **_groups(
        NETWORK_SECTION,
        ("cisco", "Cisco"),
        ("juniper", "Juniper"),
        ("mikrotik", "MikroTik"),
        ("huawei", "Huawei"),
        ("fortinet", "Fortinet"),
        ("palo-alto", "Palo Alto Networks"),
        ("check-point", "Check Point"),
        ("f5", "F5"),
        ("aruba", "Aruba"),
        ("arista", "Arista"),
        ("extreme", "Extreme Networks"),
        ("nokia", "Nokia"),
        ("dell", "Dell"),
        ("brocade", "Brocade"),
        ("hpe-network", "HPE Network OS"),
        ("cumulus-linux", "NVIDIA Cumulus Linux"),
        ("sonic", "SONiC"),
        ("sonicwall", "SonicWall"),
        ("zyxel", "Zyxel"),
        ("draytek", "DrayTek"),
        ("watchguard", "WatchGuard"),
        ("sophos", "Sophos"),
        ("fritzos", "AVM FRITZ!OS"),
        ("asuswrt", "ASUSWRT"),
        ("ubiquiti", "Ubiquiti"),
        ("openwrt", "OpenWrt"),
        ("dd-wrt", "DD-WRT"),
        ("pfsense", "pfSense"),
        ("opnsense", "OPNsense"),
        ("vyos", "VyOS"),
        ("vyatta", "Vyatta"),
        ("wrt", "WRT family"),
        ("synology", "Synology"),
        ("qnap", "QNAP"),
        ("truenas", "TrueNAS"),
        ("zte", "ZTE"),
        ("d-link", "D-Link"),
        ("tp-link", "TP-Link"),
        ("netgear", "NETGEAR"),
        ("utm", "UTM appliance"),
        ("router", "Generic router"),
        ("printer", "Printer"),
    ),
    **_groups(
        EMBEDDED_SECTION,
        ("vxworks", "VxWorks"),
        ("qnx", "QNX"),
        ("freertos", "FreeRTOS"),
        ("openembedded", "OpenEmbedded"),
        ("yocto", "Yocto"),
        ("busybox", "BusyBox"),
        ("embedded", "Generic embedded OS"),
    ),
}

# Every canonical measurement tag maps to exactly one analysis group. Product
# variants are combined only where the group represents the same vendor/family.
GROUP_ONLY_IDENTIFIERS = frozenset({"brocade", "hpe-network", "extreme", "nokia", "dell"})

TAG_TO_GROUP = {
    **{group: group for group in GROUP_INFO if group not in GROUP_ONLY_IDENTIFIERS},
    "cisco-ios": "cisco",
    "cisco-iosxe": "cisco",
    "cisco-iosxr": "cisco",
    "cisco-nxos": "cisco",
    "cisco-asa": "cisco",
    "cisco-ftd": "cisco",
    "juniper-junos": "juniper",
    "juniper-junos-evolved": "juniper",
    "juniper-screenos": "juniper",
    "mikrotik-routeros": "mikrotik",
    "mikrotik-swos": "mikrotik",
    "huawei-vrp": "huawei",
    "fortinet-fortios": "fortinet",
    "paloalto-panos": "palo-alto",
    "checkpoint-gaia": "check-point",
    "f5-bigip": "f5",
    "arubaos": "aruba",
    "arubaos-cx": "aruba",
    "arista-eos": "arista",
    "extreme-exos": "extreme",
    "nokia-sros": "nokia",
    "dell-os10": "dell",
    "brocade-fos": "brocade",
    "hpe-comware": "hpe-network",
    "hpe-procurve": "hpe-network",
    "sonicos": "sonicwall",
    "zynos": "zyxel",
    "zyxel-zld": "zyxel",
    "zyxel-uos": "zyxel",
    "drayos": "draytek",
    "watchguard-fireware": "watchguard",
    "sophos-sfos": "sophos",
    "edgeos": "ubiquiti",
    "unifi-os": "ubiquiti",
    "airos": "ubiquiti",
    "synology-dsm": "synology",
    "synology-srm": "synology",
    "qnap-qts": "qnap",
    "qnap-quts-hero": "qnap",
    "truenas-core": "truenas",
    "truenas-scale": "truenas",
}

GROUP_SCHEMA = pa.schema(
    [
        ("IP_ADDR", pa.string()),
        ("OS_GROUP", pa.string()),
    ]
)
AGGREGATE_SCHEMA = pa.schema(
    [
        ("OS_SECTION", pa.string()),
        ("OS_GROUP", pa.string()),
        ("OS_GROUP_LABEL", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("COUNT", pa.int64()),
        ("OS_GROUP_TOTAL", pa.int64()),
        ("PERCENTAGE", pa.float64()),
    ]
)

PERCENTAGE_CMAP = LinearSegmentedColormap.from_list(
    "percentage_blues",
    ("#FFFFFF", "#DEEBF7", "#9ECAE1", "#4292C6", "#08519C", "#08306B"),
)


def resolve_os_measurement_id(manifest: dict, protocol: str) -> str | None:
    section = manifest.get(protocol)
    if not isinstance(section, dict) or "os" not in section:
        return None
    measurement_id = section["os"]
    if not isinstance(measurement_id, str) or not measurement_id.strip():
        raise ValueError(f"{protocol}.os: expected a non-empty measurement id")
    return measurement_id


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _case_mapping(expression: str, mapping: dict[str, str], default: str) -> str:
    clauses = " ".join(
        f"WHEN {_sql_literal(source)} THEN {_sql_literal(target)}"
        for source, target in sorted(mapping.items())
    )
    return f"CASE lower(trim({expression})) {clauses} ELSE {default} END"


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
            "Title": "Operating-system group by IP-ID selection strategy",
            "Subject": "Row-normalized IP-ID strategy distributions by OS group",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)
    return output_path


def _validate_raw_schema(os_path: Path) -> None:
    columns = tuple(pq.read_schema(os_path).names)
    if columns != RAW_OS_COLUMNS:
        raise ValueError(
            f"{os_path}: expected current OS schema {list(RAW_OS_COLUMNS)}, got {list(columns)}"
        )


def _group_file_is_current(os_path: Path, group_path: Path) -> bool:
    metadata_path = group_path.with_suffix(".meta.json")
    if not group_path.is_file() or not metadata_path.is_file():
        return False
    try:
        metadata = json.loads(metadata_path.read_text())
    except (OSError, json.JSONDecodeError):
        return False
    source = os_path.stat()
    return tuple(pq.read_schema(group_path).names) == tuple(GROUP_SCHEMA.names) and metadata == {
        "source_mtime_ns": source.st_mtime_ns,
        "source_size": source.st_size,
        "taxonomy_version": TAXONOMY_VERSION,
    }


def write_os_groups(
    os_path: Path,
    output_path: Path,
    *,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict:
    """Validate current os.pq and write resolved IP_ADDR -> OS_GROUP rows."""
    if not os_path.is_file():
        raise FileNotFoundError(os_path)
    _validate_raw_schema(os_path)
    if _group_file_is_current(os_path, output_path):
        return {
            "os_evidence_ip_count": pq.ParquetFile(os_path).metadata.num_rows,
            "grouped_ip_count": pq.ParquetFile(output_path).metadata.num_rows,
            "reused_group_file": True,
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    source = _sql_literal(str(os_path))
    destination = _sql_literal(str(temporary))
    group_expression = _case_mapping("OS_TAG", TAG_TO_GROUP, "NULL")
    parquet_compression = compression or "uncompressed"
    con = duckdb.connect(config={"threads": threads} if threads else {})
    try:
        validation = con.execute(
            f"""
            SELECT count(*)::BIGINT,
                   count(*) FILTER (WHERE IP_ADDR IS NULL OR trim(IP_ADDR) = '')::BIGINT,
                   count(*) FILTER (
                       WHERE OS_STATUS NOT IN ('resolved', 'ambiguous', 'unclassified')
                          OR OS_STATUS IS NULL
                   )::BIGINT,
                   count(*) FILTER (
                       WHERE (OS_STATUS = 'resolved' AND (OS_TAG IS NULL OR trim(OS_TAG) = ''))
                          OR (OS_STATUS <> 'resolved' AND OS_TAG IS NOT NULL)
                   )::BIGINT,
                   list(DISTINCT lower(trim(OS_TAG))) FILTER (
                       WHERE OS_STATUS = 'resolved' AND {group_expression} IS NULL
                   )
            FROM read_parquet({source})
            """
        ).fetchone()
        evidence_rows, invalid_ips, invalid_statuses, invalid_decisions, unknown_tags = validation
        if invalid_ips:
            raise ValueError(f"{os_path}: {invalid_ips} empty IP_ADDR values")
        if invalid_statuses:
            raise ValueError(f"{os_path}: {invalid_statuses} invalid OS_STATUS values")
        if invalid_decisions:
            raise ValueError(f"{os_path}: {invalid_decisions} inconsistent OS_STATUS/OS_TAG rows")
        if unknown_tags:
            raise ValueError(f"{os_path}: unmapped OS_TAG values: {sorted(unknown_tags)}")

        con.execute(
            f"""
            COPY (
                SELECT CAST(IP_ADDR AS VARCHAR) AS IP_ADDR,
                       {group_expression} AS OS_GROUP
                FROM read_parquet({source})
                WHERE OS_STATUS = 'resolved'
            ) TO {destination}
            (FORMAT PARQUET, COMPRESSION {_sql_literal(parquet_compression)})
            """
        )
    finally:
        con.close()

    if tuple(pq.read_schema(temporary).names) != tuple(GROUP_SCHEMA.names):
        raise ValueError(f"{temporary}: unexpected processed OS-group schema")
    temporary.replace(output_path)
    source_stat = os_path.stat()
    _write_json(
        output_path.with_suffix(".meta.json"),
        {
            "source_mtime_ns": source_stat.st_mtime_ns,
            "source_size": source_stat.st_size,
            "taxonomy_version": TAXONOMY_VERSION,
        },
    )
    return {
        "os_evidence_ip_count": int(evidence_rows),
        "grouped_ip_count": pq.ParquetFile(output_path).metadata.num_rows,
        "reused_group_file": False,
    }


def aggregate_os_groups(
    strategy_path: Path,
    group_path: Path,
    output_path: Path,
    *,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict:
    """Join processed groups to strategies and write row-normalized counts."""
    for path in (strategy_path, group_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    strategy_file = _sql_literal(str(strategy_path))
    group_file = _sql_literal(str(group_path))
    con = duckdb.connect(config={"threads": threads} if threads else {})
    try:
        strategy_rows, invalid_strategy_ips, strategy_values = con.execute(
            f"""
            SELECT count(*)::BIGINT,
                   count(*) FILTER (
                       WHERE IP_ADDR IS NULL OR trim(CAST(IP_ADDR AS VARCHAR)) = ''
                   )::BIGINT,
                   list(DISTINCT upper(trim(CAST(IPID_SELECTION_STRATEGY AS VARCHAR))))
            FROM read_parquet({strategy_file})
            """
        ).fetchone()
        counts = con.execute(
            f"""
            SELECT g.OS_GROUP,
                   upper(trim(CAST(s.IPID_SELECTION_STRATEGY AS VARCHAR))) AS STRATEGY,
                   count(*)::BIGINT AS N
            FROM read_parquet({group_file}) g
            JOIN read_parquet({strategy_file}) s USING (IP_ADDR)
            GROUP BY g.OS_GROUP, STRATEGY
            """
        ).fetchall()
    finally:
        con.close()

    if strategy_rows == 0:
        raise ValueError(f"{strategy_path}: strategy result is empty")
    if invalid_strategy_ips:
        raise ValueError(f"{strategy_path}: empty IP addresses in strategy result")
    unknown_strategies = sorted(set(strategy_values or []) - set(STRATEGY_NAMES))
    if unknown_strategies:
        raise ValueError(f"{strategy_path}: unknown IP-ID strategies: {unknown_strategies}")
    unknown_groups = sorted({str(row[0]) for row in counts} - set(GROUP_INFO))
    if unknown_groups:
        raise ValueError(f"{group_path}: unknown OS_GROUP values: {unknown_groups}")
    if not counts:
        raise ValueError(f"{group_path}: no OS groups match the strategy population")

    count_map = {(str(group), str(strategy)): int(count) for group, strategy, count in counts}
    totals = {
        group: sum(count_map.get((group, strategy), 0) for strategy in HEATMAP_STRATEGIES)
        for group in {str(row[0]) for row in counts}
    }
    ordered_groups = sorted(
        totals,
        key=lambda group: (GROUP_INFO[group][0], -totals[group], GROUP_INFO[group][1]),
    )
    rows = []
    for group in ordered_groups:
        section, label = GROUP_INFO[group]
        total = totals[group]
        for strategy in HEATMAP_STRATEGIES:
            count = count_map.get((group, strategy), 0)
            rows.append(
                {
                    "OS_SECTION": section,
                    "OS_GROUP": group,
                    "OS_GROUP_LABEL": label,
                    "IPID_SELECTION_STRATEGY": strategy,
                    "COUNT": count,
                    "OS_GROUP_TOTAL": total,
                    "PERCENTAGE": 100.0 * count / total,
                }
            )
    _write_table(pa.Table.from_pylist(rows, schema=AGGREGATE_SCHEMA), output_path, compression)
    return {
        "strategy_ip_count": int(strategy_rows),
        "grouped_ip_count": pq.ParquetFile(group_path).metadata.num_rows,
        "matched_grouped_ip_count": sum(totals.values()),
        "unmatched_grouped_ip_count": pq.ParquetFile(group_path).metadata.num_rows
        - sum(totals.values()),
        "not_enough_samples_ip_count": sum(
            count_map.get((group, "NOT_ENOUGH_SAMPLES"), 0) for group in totals
        ),
        "os_group_count": len(totals),
        "groups": {
            group: {
                "section": GROUP_INFO[group][0],
                "label": GROUP_INFO[group][1],
                "ip_count": totals[group],
            }
            for group in ordered_groups
        },
    }


def _format_ip_count(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}k"
    return str(value)


def plot_os_groups(aggregate_path: Path, output_path: Path) -> Path:
    rows = pq.read_table(aggregate_path).to_pylist()
    if not rows:
        raise ValueError(f"{aggregate_path}: no grouped OS strategy rows to plot")
    values = {
        (row["OS_GROUP"], row["IPID_SELECTION_STRATEGY"]): float(row["PERCENTAGE"]) for row in rows
    }
    totals = {row["OS_GROUP"]: int(row["OS_GROUP_TOTAL"]) for row in rows}
    sections = []
    for section in (GENERAL_PURPOSE_SECTION, NETWORK_SECTION, EMBEDDED_SECTION):
        groups = sorted(
            (group for group in totals if GROUP_INFO[group][0] == section),
            key=lambda group: (-totals[group], GROUP_INFO[group][1].casefold()),
        )
        if groups:
            sections.append((section, groups))

    configure_paper_style()
    height = max(3.8, 0.30 * len(totals) + 1.9)
    fig, axes = plt.subplots(
        nrows=len(sections),
        sharex=True,
        squeeze=False,
        figsize=(7.16, height),
        gridspec_kw={
            "height_ratios": [len(groups) for _, groups in sections],
            "hspace": 0.31,
        },
    )
    image = None
    for index, ((section, groups), axis) in enumerate(zip(sections, axes[:, 0], strict=True)):
        matrix = np.asarray(
            [
                [values.get((group, strategy), 0.0) for strategy in HEATMAP_STRATEGIES]
                for group in groups
            ]
        )
        image = axis.imshow(
            matrix,
            cmap=PERCENTAGE_CMAP,
            vmin=0,
            vmax=100,
            aspect="auto",
            interpolation="nearest",
        )
        axis.set_yticks(
            np.arange(len(groups)),
            [f"{GROUP_INFO[g][1]} ({_format_ip_count(totals[g])})" for g in groups],
        )
        axis.set_title(
            section,
            pad=6,
            fontproperties=linux_libertine_font_properties("DR", size=11),
        )
        last = index == len(sections) - 1
        axis.tick_params(axis="x", bottom=last, labelbottom=last)
        for row_index in range(matrix.shape[0]):
            for column_index in range(matrix.shape[1]):
                percentage = matrix[row_index, column_index]
                axis.text(
                    column_index,
                    row_index,
                    "-" if percentage == 0 else f"{percentage:.1f}",
                    ha="center",
                    va="center",
                    color="white" if percentage >= 50 else "#222222",
                    fontsize=8,
                )
    labels = [STRATEGY_PRETTY[strategy] for strategy in HEATMAP_STRATEGIES]
    axes[-1, 0].set_xticks(
        np.arange(len(labels)), labels, rotation=35, ha="right", rotation_mode="anchor"
    )
    axes[-1, 0].set_xlabel("IP-ID Selection Strategy")
    fig.supylabel("Operating-System Group (#IP Addr.)", x=0.018)
    fig.subplots_adjust(left=0.31, right=0.86, bottom=0.25, top=0.94)
    assert image is not None
    colorbar_axis = fig.add_axes([0.885, 0.30, 0.018, 0.48])
    colorbar = fig.colorbar(image, cax=colorbar_axis, ticks=np.arange(0, 101, 20))
    colorbar.set_label("Percentage [%]")
    return _save_figure(fig, output_path)


def _metadata(stats: dict, *, strategy_input: str, group_path: Path) -> dict:
    return {
        "methodology": {
            "strategy_input": strategy_input,
            "os_input": "resolved OS_TAG mapped surjectively to OS_GROUP by IP_ADDR",
            "normalization": "each OS-group row is normalized independently to 100%",
            "taxonomy_version": TAXONOMY_VERSION,
            "zero_cell_label": "-",
            "percentage_decimals": 1,
        },
        "processed_os_groups": str(group_path),
        **stats,
    }


def _group_path(processed_root: Path, os_measurement_id: str) -> Path:
    return processed_root / "os" / os_measurement_id / OS_GROUP_INPUT_NAME


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
    if (
        merge.protocol not in SUPPORTED_PROTOCOLS
        or merge.connection_mode != "no-connection"
        or merge.base.interval != "rt-based"
        or merge.mass.interval != "fixed-interval"
    ):
        raise ValueError("OS group heatmap requires a no-connection RT-base/fixed-mass merge")
    if not os_measurement_id.strip():
        raise ValueError("OS measurement id must not be empty")

    strategy_path = merge.artifact_path(processed_root, "strategies")
    os_path = raw_root / "os" / os_measurement_id / OS_INPUT_NAME
    group_path = _group_path(processed_root, os_measurement_id)
    aggregate_path = merge.artifact_path(processed_root, KIND)
    pdf_path = merge.artifact_path(figures_root, KIND, "pdf")
    json_path = merge.artifact_path(figures_root, KIND, "json")
    group_stats = write_os_groups(os_path, group_path, compression=compression, threads=threads)
    stats = aggregate_os_groups(
        strategy_path, group_path, aggregate_path, compression=compression, threads=threads
    )
    plot_os_groups(aggregate_path, pdf_path)
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
            "sources": {"strategies": str(strategy_path), "os": str(os_path)},
            "aggregate": str(aggregate_path),
            "figure": KIND,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            **_metadata(
                {**group_stats, **stats}, strategy_input="merged strategies", group_path=group_path
            ),
        },
    )
    return pdf_path, json_path, aggregate_path


def render_measurement(
    measurement: IpidMeasurement,
    os_measurement_id: str,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    raw_root: Path = RAW_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> tuple[Path, Path, Path]:
    if (
        measurement.protocol != "tcp"
        or measurement.connection_mode != "connection"
        or measurement.interval != "rt-based"
        or measurement.scale != "base"
    ):
        raise ValueError("connection OS group heatmap requires tcp.ipid.connection.rt-based.base")
    if not os_measurement_id.strip():
        raise ValueError("OS measurement id must not be empty")

    strategy_path = measurement.artifact_path(processed_root, "strategies")
    os_path = raw_root / "os" / os_measurement_id / OS_INPUT_NAME
    group_path = _group_path(processed_root, os_measurement_id)
    aggregate_path = measurement.artifact_path(processed_root, KIND)
    pdf_path = measurement.artifact_path(figures_root, KIND, "pdf")
    json_path = measurement.artifact_path(figures_root, KIND, "json")
    group_stats = write_os_groups(os_path, group_path, compression=compression, threads=threads)
    stats = aggregate_os_groups(
        strategy_path, group_path, aggregate_path, compression=compression, threads=threads
    )
    plot_os_groups(aggregate_path, pdf_path)
    _write_json(
        json_path,
        {
            "target": measurement.target,
            "protocol": measurement.protocol,
            "connection_mode": measurement.connection_mode,
            "zmap_id": measurement.zmap_id,
            "os_measurement_id": os_measurement_id,
            "measurements": {"rt_based_base": measurement.measurement_id},
            "sources": {"strategies": str(strategy_path), "os": str(os_path)},
            "aggregate": str(aggregate_path),
            "figure": KIND,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            **_metadata(
                {**group_stats, **stats},
                strategy_input="TCP connection-oriented RT-based base classification",
                group_path=group_path,
            ),
        },
    )
    return pdf_path, json_path, aggregate_path


@app.command()
def main(
    base_target: str = typer.Argument(..., help="RT-based base measurement target"),
    mass_target: str | None = typer.Argument(None, help="fixed-interval mass target"),
    manifest: Path = typer.Option(  # noqa: B008
        DEFAULT_MANIFEST, help="measurement manifest JSON"
    ),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, min=0, help="DuckDB threads; 0 uses all cores"),
) -> None:
    try:
        manifest_data = load_manifest(manifest)
        if mass_target is None:
            measurement = resolve(manifest_data, base_target)
            if measurement is None:
                raise ValueError(f"{base_target}: not present in manifest")
            os_measurement_id = resolve_os_measurement_id(manifest_data, measurement.protocol)
            if os_measurement_id is None:
                raise ValueError(f"{measurement.protocol}.os: not present in manifest")
            outputs = render_measurement(
                measurement,
                os_measurement_id,
                compression=None if compression == "none" else compression,
                threads=threads,
            )
            target = measurement.target
        else:
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
            target = merge.target
    except (FileNotFoundError, ValueError) as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc
    logger.success(f"[{target}] OS group strategy heatmap -> {outputs[0]}")


if __name__ == "__main__":
    app()
