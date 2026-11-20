"""Shared display-only rounding and spacing for the paper heatmaps."""

from decimal import ROUND_HALF_UP, Decimal
from numbers import Integral

COLORBAR_LABEL_PAD_POINTS = 0
MAX_COLORBAR_GAP_POINTS = 4


def round_half_away(value, places=0):
    """Round the decimal value, including negative ties, away from zero."""
    number = Decimal(str(value))
    return number.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def format_population(value):
    """Use exact IP counts; never round a previously abbreviated label."""
    if isinstance(value, bool) or not isinstance(value, Integral) or value < 0:
        raise ValueError("expected a nonnegative integer IP population")
    value = int(value)
    if value < 1000:
        return str(value)
    if value >= 1_000_000:
        number, places, suffix = Decimal(value) / 1_000_000, 1, "M"
    else:
        number, places, suffix = Decimal(value) / 1000, int(value < 100_000), "k"
        if round_half_away(number, places) == 1000:
            return "1M"
    text = format(round_half_away(number, places), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text + suffix


def format_percentage(value, count):
    """Preserve exact-zero dashes and numeric small-positive 0.0 labels."""
    if count == 0:
        return "-"
    rounded = round_half_away(value, 1)
    return "100" if rounded == 100 else format(rounded, ".1f")


def colorbar_gap_points(preferred):
    """Four points is a cap, not a replacement for already smaller gaps."""
    return min(preferred, MAX_COLORBAR_GAP_POINTS)
