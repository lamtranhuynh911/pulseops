# Mock datasets

Mock test results from machines 1–3 for two scenarios. Each scenario has one CSV file per machine, with 60 rows per file.

```
mock-data/
├── generate.py          Regenerates all the CSV files
├── ideal/               Every product passes every machine
│   ├── machine-1.csv
│   ├── machine-2.csv
│   └── machine-3.csv
└── qa-violations/       Products bypass or fail machine 1 or 2 and still reach later machines
    ├── machine-1.csv
    ├── machine-2.csv
    └── machine-3.csv
```

## Columns

Each row is one test result, the same data a machine publishes to the `machine.test-results` topic (see [Messages](../README.md#messages) in the main README):

```csv
event_id,machine_id,product_id,result,timestamp,cycle_time_s,temperature_c,length_mm
5875d3a7-f92b-4357-8fdc-400cf1c2bd75,machine-1,P-000001,PASS,2026-10-15T08:00:07.041Z,7.0,34.1,120.045
```

| Column | Description |
|---|---|
| `event_id` | Unique message ID (UUID) |
| `machine_id` | `machine-1`, `machine-2`, or `machine-3` |
| `product_id` | Product that was tested |
| `result` | `PASS` or `FAIL` |
| `timestamp` | When the test finished, ISO 8601 UTC |
| `cycle_time_s` | How long the test took, in seconds |
| `temperature_c` | Machine temperature, in °C |
| last column | The value measured by the machine's test, which decides the result (see below) |

| Machine | Test | Last column | `PASS` when |
|---|---|---|---|
| machine-1 | Dimensional check | `length_mm` | 119.850 to 120.150 mm |
| machine-2 | Leak test | `leak_rate_ml_min` | at most 0.50 mL/min |
| machine-3 | Functional test | `output_voltage_v` | 4.900 to 5.100 V |

The tests are invented to make the data look realistic; rename them to match the real machines.

To turn a row into a Kafka message, move `cycle_time_s`, `temperature_c`, and the last column into `measurements`, and add `"schema_version": 1`.

## Timing

A new product enters machine 1 every 10 seconds and reaches each following machine 15 seconds later, so the three files of a scenario can be merged by `timestamp` and replayed in order.

The two scenarios use different times and product numbers, so both can be loaded into the same database without mixing:

| Scenario | Time on 2026-10-15 (UTC) | Products |
|---|---|---|
| `ideal` | 08:00:07 to 08:10:26 | `P-000001` to `P-000060` |
| `qa-violations` | 09:00:06 to 09:10:56 | `P-000101` to `P-000163` |

The files hold test results only, no heartbeats. When replaying `qa-violations`, also send a heartbeat for each machine every 5 seconds, as real machines would: where a product is missing from a file, consecutive rows are up to about 20 seconds apart, which is longer than the 15-second integrity timeout. In `ideal`, rows are never more than about 11 seconds apart.

To replay a scenario into the running stack (with heartbeats), use the replay Job described in [Replaying mock data](../README.md#replaying-mock-data) in the main README.

## Scenario 1: `ideal`

60 products go through all three machines and pass every test, so all three files contain the same 60 products.

Expected: no QA violations, all 60 products `PASSED`, and all three machines `UP`.

## Scenario 2: `qa-violations`

Some products bypass machine 1 or 2, or fail there, and still reach the later machines. For contrast, a few products fail and correctly leave the line, which the QA check must not flag. It takes 63 products for every machine to still have 60 rows.

In the table, "—" means the machine has no row for the product.

| Products | machine-1 | machine-2 | machine-3 | Expected |
|---|---|---|---|---|
| P-000108, P-000131, P-000152 | — | PASS | PASS | `MISSING_UPSTREAM` at machine-2 (bypassed machine 1) |
| P-000115, P-000144 | PASS | — | PASS | `MISSING_UPSTREAM` at machine-3 (bypassed machine 2) |
| P-000112, P-000127, P-000158 | FAIL | PASS | PASS | `FAILED_UPSTREAM` at machine-2 |
| P-000120, P-000137, P-000149 | PASS | FAIL | PASS | `FAILED_UPSTREAM` at machine-3 |
| P-000124 | FAIL | — | — | No violation: rejected at machine 1 |
| P-000105, P-000141 | PASS | FAIL | — | No violation: rejected at machine 2 |
| P-000133 | PASS | PASS | FAIL | No violation: rejected at machine 3 |
| The other 48 | PASS | PASS | PASS | No violation |

Expected totals:

- 11 QA violations: 5 `MISSING_UPSTREAM` and 6 `FAILED_UPSTREAM`. A `MISSING_UPSTREAM` is raised only once the `QA_GRACE_SECONDS` grace period has passed.
- Product status: 48 `PASSED`, 4 `FAILED`, 11 `NON_CONFORMING`.
- Results: machine-1 has 56 `PASS` and 4 `FAIL`, machine-2 has 55 and 5, machine-3 has 59 and 1.

## Regenerating the files

```bash
python3 mock-data/generate.py
```

The script needs only the Python standard library. Its random seed is fixed, so it rewrites identical files every time. To change a scenario, edit `SCENARIOS` in the script; it refuses to write the files unless every machine ends up with exactly 60 rows.
