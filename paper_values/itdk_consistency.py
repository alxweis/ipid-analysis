"""Router node agreement from existing small ITDK summaries; no per-IP reads."""

import math

from paper_values.readers import percentage


def router_strict_agreement(data):
    """One vote per eligible node with T evidence on a classified interface."""
    fields = ("transit_evidenced_eligible_nodes", "transit_evidenced_strict_nodes")
    present = [key in data for key in fields]
    if not any(present):
        return None  # Older summaries must not fall back to the all-node rate.
    if not all(present):
        raise ValueError("incomplete router ITDK consistency counts")
    eligible, strict = (data[key] for key in fields)
    if type(eligible) is not int or type(strict) is not int or not 0 <= strict <= eligible:
        raise ValueError("invalid router ITDK consistency counts")
    for key, subset in (
        ("eligible_nodes_with_two_classified_interfaces", eligible),
        ("strict_agreement_nodes", strict),
    ):
        if key in data and (type(data[key]) is not int or data[key] < subset):
            raise ValueError("router ITDK counts exceed the all-node population")
    if "transit_evidenced_strict_percentage" in data:
        reported = data["transit_evidenced_strict_percentage"]
        expected = percentage(strict, eligible) if eligible else 0.0
        if (
            type(reported) not in (int, float)
            or not math.isfinite(reported)
            or not math.isclose(reported, expected, rel_tol=1e-9, abs_tol=1e-9)
        ):
            raise ValueError("router ITDK percentage differs from its counts")
    return percentage(strict, eligible)  # No eligible routers is unavailable, not 0%.


def read_router_agreement(spec, site, readers):
    artifact = readers.report(spec, site)
    if artifact is None:
        return None, ()
    return router_strict_agreement(artifact.data), (artifact.name,)
