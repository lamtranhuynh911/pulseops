"""Spec limits of each machine's test measurement, and the zone a measured value falls in.

The limits are the ones in the README ("Messages") and mock-data/README.md; keep them in sync with the
real machines. A value inside the spec but within MARGINAL_FRACTION of a limit is "marginal": it still
passes, but a drift towards that limit shows up before results start to FAIL.
"""

OK = "ok"
MARGINAL = "marginal"
OUT = "out"

# Marginal: within 20 % of the half tolerance of a two-sided limit (119.850-120.150 -> below 119.880 or
# above 120.120), or within 20 % of a one-sided limit (at most 0.50 -> above 0.40).
MARGINAL_FRACTION = 0.2

# (machine, measurement) -> (lower limit or None, upper limit or None)
SPEC_LIMITS: dict[tuple[str, str], tuple[float | None, float | None]] = {
    ("machine-1", "length_mm"): (119.850, 120.150),
    ("machine-2", "leak_rate_ml_min"): (None, 0.50),
    ("machine-3", "output_voltage_v"): (4.900, 5.100),
}


def zone(machine_id: str, name: str, value: float) -> str | None:
    """The spec zone of a measurement, or None if the measurement has no spec (e.g. temperature_c)."""
    limits = SPEC_LIMITS.get((machine_id, name))
    if limits is None:
        return None
    low, high = limits
    if (low is not None and value < low) or (high is not None and value > high):
        return OUT
    if low is not None and high is not None:
        band = (high - low) / 2 * MARGINAL_FRACTION
    else:
        band = abs(high if high is not None else low) * MARGINAL_FRACTION
    if (low is not None and value < low + band) or (high is not None and value > high - band):
        return MARGINAL
    return OK
