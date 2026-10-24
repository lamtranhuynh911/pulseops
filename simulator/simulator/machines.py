"""The simulated machines and their tests.

Copied from MACHINES in mock-data/generate.py, which is a separate script outside this image.
Keep the two tables in sync, in particular the measurement names, which must match the README.
"""

import random
from dataclasses import dataclass

STAGE_S = 15  # a product reaches each following machine 15 s after the previous one


@dataclass(frozen=True)
class Machine:
    id: str
    cycle_time_s: float                       # nominal test duration
    temperature_c: float                      # nominal machine temperature
    measurement: str                          # name of the value the test measures
    decimals: int
    passing: tuple[float, float, float]       # low, high, and most likely in-spec value
    failing: tuple[tuple[float, float], ...]  # value ranges outside the spec limits

    def measure(self, result: str, rng: random.Random) -> float:
        if result == "PASS":
            value = rng.triangular(*self.passing)
        else:
            value = rng.uniform(*rng.choice(self.failing))
        return round(value, self.decimals)

    def measurements(self, result: str, rng: random.Random) -> dict[str, float]:
        return {
            "cycle_time_s": round(rng.gauss(self.cycle_time_s, 0.3), 1),
            "temperature_c": round(rng.gauss(self.temperature_c, 0.4), 1),
            self.measurement: self.measure(result, rng),
        }


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
