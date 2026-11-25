"""An isolated OS taxonomy for additional adjusted figures; defaults stay intact."""

from ipid_analysis.plot_os_group_strategy import (
    GROUP_INFO,
    PAPER_OS_GROUP_COLORS,
    PAPER_OS_GROUP_ORDER,
    TAG_TO_GROUP,
)

VERSION = "paper-os-adjusted-v1"
SPLITS = {
    "mikrotik": ("mikrotik-routeros", "mikrotik-swos"),
    "hpe-network": ("hpe-comware", "hpe-network"),
}
LABELS = {
    "mikrotik-routeros": "MikroTik RouterOS",
    "mikrotik-swos": "MikroTik SwOS",
    "hpe-comware": "HPE Comware",
    "hpe-network": "HPE Network OS",
}
GROUPS = {}
for old, info in GROUP_INFO.items():
    for key in SPLITS.get(old, (old,)):
        GROUPS[key] = (info[0], LABELS.get(key, info[1]))

TAGS = {tag: SPLITS[group][1] if group in SPLITS else group for tag, group in TAG_TO_GROUP.items()}
TAGS.update({"mikrotik-routeros": "mikrotik-routeros", "hpe-comware": "hpe-comware"})
ORDER = tuple(key for old in PAPER_OS_GROUP_ORDER for key in SPLITS.get(old, (old,)))
COLORS = {key: PAPER_OS_GROUP_COLORS[old] for old in GROUP_INFO for key in SPLITS.get(old, (old,))}


# Related shades distinguish the remainders without recoloring unrelated groups.
def lighter(color):
    return "#" + "".join(
        f"{round(int(color[i : i + 2], 16) * 0.7 + 255 * 0.3):02X}" for i in (1, 3, 5)
    )


COLORS.update(
    {
        "mikrotik-swos": lighter(PAPER_OS_GROUP_COLORS["mikrotik"]),
        "hpe-network": lighter(PAPER_OS_GROUP_COLORS["hpe-network"]),
        "__other__": "#D0C8C4",
    }
)
HEATMAP_Y_LABEL = "Operating-System Group / Vendor (#IP Addr.)"
BAR_X_LABEL = "Operating-System Group / Vendor [%]"
NETWORK_HEATMAP_GROUPS = (
    "cisco",
    "huawei",
    "zte",
    "hpe-comware",
    "aruba",
    "mikrotik-routeros",
    "draytek",
    "zyxel",
    "tp-link",
    "f5",
    "sonicwall",
    "check-point",
    "pfsense",
    "opnsense",
)


def parent(group):
    if group in SPLITS["mikrotik"]:
        return "mikrotik"
    if group in SPLITS["hpe-network"]:
        return "hpe-network"
    return group


def selection(original):
    return tuple(
        (
            title,
            NETWORK_HEATMAP_GROUPS
            if title == "Network OS"
            else tuple(key for old in keys for key in SPLITS.get(old, (old,))),
        )
        for title, keys in original
    )
