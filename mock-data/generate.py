"""Generate the mock machine datasets in this folder.

Writes one CSV file per machine for each scenario, 60 test results per file.
The random seed is fixed, so re-running the script rewrites the same files.

Usage: python3 mock-data/generate.py
"""

import csv
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUT_DIR = Path(__file__).parent
SEED = 42
ROWS_PER_MACHINE = 60
TAKT_S = 10   # a new product enters machine 1 every 10 s...
STAGE_S = 15  # ...and reaches each following machine 15 s later


@dataclass(frozen=True)
class Machine:
    id: str
    cycle_time_s: float                       # nominal test duration
    temperature_c: float                      # nominal machine temperature
    measurement: str                          # CSV column for the value the test measures
    decimals: int
    passing: tuple[float, float, float]       # low, high, and most likely in-spec value
    failing: tuple[tuple[float, float], ...]  # value ranges outside the spec limits

    def measure(self, result: str, rng: random.Random) -> float:
        if result == "PASS":
            return rng.triangular(*self.passing)
        return rng.uniform(*rng.choice(self.failing))


MACHINES = (
    Machine(  # dimensional check, spec 120.000 ± 0.150 mm
        "machine-1", cycle_time_s=7.0, temperature_c=34.0, measurement="length_mm", decimals=3,
        passing=(119.88, 120.12, 120.0), failing=((119.70, 119.82), (120.18, 120.30)),
    ),
    Machine(  # leak test, spec at most 0.50 mL/min
        "machine-2", cycle_time_s=8.5, temperature_c=36.0, measurement="leak_rate_ml_min", decimals=2,
        passing=(0.05, 0.30, 0.12), failing=((0.70, 1.60),),
    ),
    Machine(  # functional test, spec 5.000 ± 0.100 V
        "machine-3", cycle_time_s=6.0, temperature_c=41.0, measurement="output_voltage_v", decimals=3,
        passing=(4.94, 5.06, 5.0), failing=((4.75, 4.88), (5.12, 5.25)),
    ),
)

# A journey is the result a product gets at machines 1, 2 and 3. None means the
# machine has no record of the product: it was bypassed, or it had already left
# the line after failing an earlier test.
NORMAL = ("PASS", "PASS", "PASS")


@dataclass(frozen=True)
class Scenario:
    name: str
    start: datetime
    products: range                              # product numbers, in the order they enter the line
    journeys: dict[int, tuple[str | None, ...]]  # products that don't follow NORMAL


SCENARIOS = (
    Scenario("ideal", datetime(2026, 10, 15, 8, 0, tzinfo=timezone.utc), range(1, 61), {}),
    Scenario(
        "qa-violations",
        datetime(2026, 10, 15, 9, 0, tzinfo=timezone.utc),
        range(101, 164),  # 63 products, so that every machine still records 60 results
        {
            # QA violations: the product reached a machine without passing the previous one
            108: (None, "PASS", "PASS"),    # bypassed machine 1
            131: (None, "PASS", "PASS"),
            152: (None, "PASS", "PASS"),
            115: ("PASS", None, "PASS"),    # bypassed machine 2
            144: ("PASS", None, "PASS"),
            112: ("FAIL", "PASS", "PASS"),  # failed machine 1 but kept moving
            127: ("FAIL", "PASS", "PASS"),
            158: ("FAIL", "PASS", "PASS"),
            120: ("PASS", "FAIL", "PASS"),  # failed machine 2 but kept moving
            137: ("PASS", "FAIL", "PASS"),
            149: ("PASS", "FAIL", "PASS"),
            # Correct rejects, for contrast: the product failed and left the line
            124: ("FAIL", None, None),
            105: ("PASS", "FAIL", None),
            141: ("PASS", "FAIL", None),
            133: ("PASS", "PASS", "FAIL"),
        },
    ),
)


def generate(scenario: Scenario, rng: random.Random) -> dict[str, list[dict[str, str]]]:
    """Return each machine's rows, in timestamp order."""
    rows: dict[str, list[dict[str, str]]] = {machine.id: [] for machine in MACHINES}
    for position, number in enumerate(scenario.products):
        entered = scenario.start + timedelta(seconds=position * TAKT_S)
        journey = scenario.journeys.get(number, NORMAL)
        for stage, (machine, result) in enumerate(zip(MACHINES, journey)):
            if result is None:
                continue
            cycle_time = round(rng.gauss(machine.cycle_time_s, 0.3), 1)
            finished = entered + timedelta(seconds=stage * STAGE_S + cycle_time, milliseconds=rng.randrange(100))
            rows[machine.id].append({
                "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                "machine_id": machine.id,
                "product_id": f"P-{number:06d}",
                "result": result,
                "timestamp": finished.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "cycle_time_s": f"{cycle_time:.1f}",
                "temperature_c": f"{rng.gauss(machine.temperature_c, 0.4):.1f}",
                machine.measurement: f"{machine.measure(result, rng):.{machine.decimals}f}",
            })
    return rows


def main() -> None:
    for scenario in SCENARIOS:
        rows = generate(scenario, random.Random(f"{SEED}:{scenario.name}"))
        for machine_id, machine_rows in rows.items():
            if len(machine_rows) != ROWS_PER_MACHINE:
                raise SystemExit(
                    f"{scenario.name}: {machine_id} has {len(machine_rows)} rows, expected {ROWS_PER_MACHINE}"
                )
        for machine_id, machine_rows in rows.items():
            path = OUT_DIR / scenario.name / f"{machine_id}.csv"
            path.parent.mkdir(exist_ok=True)
            with path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(machine_rows[0]), lineterminator="\n")
                writer.writeheader()
                writer.writerows(machine_rows)
            print(f"Wrote {path.relative_to(OUT_DIR.parent)}")


if __name__ == "__main__":
    main()
