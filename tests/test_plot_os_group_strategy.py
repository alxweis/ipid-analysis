from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from matplotlib.font_manager import FontProperties
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.plot_os_group_strategy import (
    GROUP_INFO,
    RAW_OS_COLUMNS,
    TAG_TO_GROUP,
    aggregate_os_groups,
    plot_os_groups,
    write_os_groups,
)

MEASUREMENT_TAGS = {
    "cisco-iosxe",
    "cisco-iosxr",
    "cisco-nxos",
    "cisco-ftd",
    "cisco-asa",
    "cisco-ios",
    "juniper-junos-evolved",
    "juniper-screenos",
    "juniper-junos",
    "mikrotik-routeros",
    "mikrotik-swos",
    "huawei-vrp",
    "fortinet-fortios",
    "paloalto-panos",
    "checkpoint-gaia",
    "f5-bigip",
    "arubaos-cx",
    "arubaos",
    "arista-eos",
    "extreme-exos",
    "nokia-sros",
    "dell-os10",
    "brocade-fos",
    "hpe-comware",
    "hpe-procurve",
    "cumulus-linux",
    "sonicos",
    "sonic",
    "zynos",
    "zyxel-zld",
    "zyxel-uos",
    "drayos",
    "watchguard-fireware",
    "sophos-sfos",
    "fritzos",
    "asuswrt",
    "edgeos",
    "unifi-os",
    "airos",
    "openwrt",
    "dd-wrt",
    "wrt",
    "pfsense",
    "opnsense",
    "vyos",
    "vyatta",
    "synology-dsm",
    "synology-srm",
    "qnap-quts-hero",
    "qnap-qts",
    "truenas-core",
    "truenas-scale",
    "vmware-esxi",
    "proxmox-ve",
    "raspbian",
    "ubuntu",
    "debian",
    "almalinux",
    "rocky-linux",
    "oracle-linux",
    "amazon-linux",
    "kali-linux",
    "linux-mint",
    "manjaro",
    "nixos",
    "clear-linux",
    "photon-os",
    "flatcar",
    "coreos",
    "slackware",
    "centos",
    "rhel",
    "fedora",
    "opensuse",
    "suse",
    "euleros",
    "zorin",
    "alpine",
    "arch-linux",
    "gentoo",
    "openembedded",
    "yocto",
    "freebsd",
    "openbsd",
    "netbsd",
    "macos",
    "apple-ios",
    "android",
    "chromeos",
    "solaris",
    "aix",
    "hpux",
    "zos",
    "openvms",
    "vxworks",
    "qnx",
    "freertos",
    "windows",
    "apple",
    "cisco",
    "juniper",
    "mikrotik",
    "huawei",
    "fortinet",
    "palo-alto",
    "check-point",
    "f5",
    "aruba",
    "arista",
    "sonicwall",
    "zyxel",
    "draytek",
    "watchguard",
    "sophos",
    "ubiquiti",
    "synology",
    "qnap",
    "truenas",
    "zte",
    "d-link",
    "tp-link",
    "netgear",
    "busybox",
    "bsd",
    "linux",
    "utm",
    "embedded",
    "printer",
    "router",
    "server",
}


class OSGroupStrategyPlotTest(unittest.TestCase):
    @staticmethod
    def _write(path: Path, columns: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(columns), path)

    @classmethod
    def _write_os(cls, path: Path, addresses: list[str], statuses: list[str], tags: list):
        columns = {
            "IP_ADDR": addresses,
            "OS_STATUS": statuses,
            "OS_TAG": tags,
        }
        for name in RAW_OS_COLUMNS[3:]:
            columns[name] = [None] * len(addresses)
        cls._write(path, columns)

    def test_every_measurement_tag_has_exactly_one_group(self):
        self.assertEqual(set(TAG_TO_GROUP), MEASUREMENT_TAGS)
        self.assertTrue(set(TAG_TO_GROUP.values()) <= set(GROUP_INFO))
        self.assertNotEqual(TAG_TO_GROUP["freebsd"], TAG_TO_GROUP["openbsd"])
        self.assertNotEqual(TAG_TO_GROUP["sonic"], TAG_TO_GROUP["sonicos"])
        self.assertEqual(TAG_TO_GROUP["apple"], "apple")

    def test_writes_groups_and_aggregates_only_resolved_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            addresses = [f"192.0.2.{index}" for index in range(1, 7)]
            os_path = root / "os.pq"
            group_path = root / "os-groups.pq"
            strategy_path = root / "strategies.pq"
            aggregate_path = root / "aggregate.pq"
            self._write_os(
                os_path,
                addresses,
                ["resolved", "resolved", "resolved", "ambiguous", "unclassified", "resolved"],
                ["ubuntu", "cisco-iosxe", "cisco-nxos", None, None, "openbsd"],
            )
            self._write(
                strategy_path,
                {
                    "IP_ADDR": addresses,
                    "IPID_SELECTION_STRATEGY": [
                        "CONSTANT",
                        "SINGLE",
                        "RANDOM",
                        "MULTI",
                        "UNCLASSIFIED",
                        "PER_BUCKET",
                    ],
                },
            )

            group_stats = write_os_groups(os_path, group_path)
            self.assertEqual(group_stats["os_evidence_ip_count"], 6)
            self.assertEqual(group_stats["grouped_ip_count"], 4)
            self.assertTrue(write_os_groups(os_path, group_path)["reused_group_file"])
            groups = {
                row["IP_ADDR"]: row["OS_GROUP"] for row in pq.read_table(group_path).to_pylist()
            }
            self.assertEqual(groups[addresses[0]], "ubuntu")
            self.assertEqual(groups[addresses[1]], "cisco")
            self.assertEqual(groups[addresses[2]], "cisco")
            self.assertEqual(groups[addresses[5]], "openbsd")

            stats = aggregate_os_groups(strategy_path, group_path, aggregate_path)
            self.assertEqual(stats["matched_grouped_ip_count"], 4)
            rows = pq.read_table(aggregate_path).to_pylist()
            cisco = {
                row["IPID_SELECTION_STRATEGY"]: row["PERCENTAGE"]
                for row in rows
                if row["OS_GROUP"] == "cisco"
            }
            self.assertEqual(cisco["SINGLE"], 50.0)
            self.assertEqual(cisco["RANDOM"], 50.0)

            figure = root / "groups.pdf"
            with (
                patch("ipid_analysis.plot_os_group_strategy.configure_paper_style"),
                patch(
                    "ipid_analysis.plot_os_group_strategy.linux_libertine_font_properties",
                    return_value=FontProperties(),
                ),
            ):
                plot_os_groups(aggregate_path, figure)
            self.assertTrue(figure.is_file())

    def test_rejects_old_or_unknown_schema_and_tags(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_path = root / "old.pq"
            self._write(old_path, {"IP_ADDR": ["192.0.2.1"], "OS_NAME": ["ubuntu"]})
            with self.assertRaisesRegex(ValueError, "expected current OS schema"):
                write_os_groups(old_path, root / "groups.pq")

            new_path = root / "new.pq"
            self._write_os(new_path, ["192.0.2.1"], ["resolved"], ["unknown-os"])
            with self.assertRaisesRegex(ValueError, "unmapped OS_TAG"):
                write_os_groups(new_path, root / "groups.pq")


if __name__ == "__main__":
    unittest.main()
