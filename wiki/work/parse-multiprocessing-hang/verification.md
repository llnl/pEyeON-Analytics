---
title: "Parse Multiprocessing Hang: Verification"
type: component
confidence: high
grounded_by:
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/tests/testParse.py
policy: agent-editable
last_validated: 2026-08-03
component: pEyeON-core
tags: [parse, multiprocessing, verification]
---

# Parse Multiprocessing Hang: Verification

## Diagnostic Findings

- Full `eyeon parse -t 8` over `~/data/eyeon-brew-bins/` stalled near completion with large Mach-O binaries remaining.
- Single-file `eyeon observe -v DEBUG` for `/source/coder` completed initialization; `mach_o_file` took under one second, `native_lib_file` took about 19.5 seconds, and `ssdeep` took about 1.3 seconds.
- A serialized run over `coder`, `helm`, `k9s`, `parqeye`, and `vcluster` completed.
- A five-file parallel run before the fix stalled at 4/5 with `coder` missing from `hang-debug-five` output.
- After the spawn/worker-recycle and Surfactant DB warmup fixes, rebuilt-image full parses still stalled at 102/104 with `coder` and `k9s` missing.

## Commands Run

- In `../pEyeON`: `uv run python -m py_compile src/eyeon/parse.py tests/testParse.py`
- In `../pEyeON`: `uv run python -m unittest tests.testParse.TestParseFunctions tests.testParse.X86TwoThreadTestCase tests.testParse.X86ThreeThreadTestCase`
- In `../pEyeON`: `uv run eyeon parse -v WARNING -o /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-five-results -t 5 /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-five-source`
- In `../pEyeON`: `bash -n eyeon-parse.sh`
- In `../pEyeON`: `bash -n builds/provision/warm-surfactant-dbs.sh`
- In this repo: `bash -n eyeon-parse.sh`
- In `../pEyeON`: `uv run eyeon parse -v WARNING -o /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-five-results-large-serial -t 5 /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-five-source`

## Results

- Python compile check passed.
- Targeted unit tests passed: 13 tests in `tests.testParse.TestParseFunctions`, `tests.testParse.X86TwoThreadTestCase`, and `tests.testParse.X86ThreeThreadTestCase`.
- Local five-file multiprocessing smoke test completed 5/5 and wrote JSON for `coder`, `helm`, `k9s`, `parqeye`, and `vcluster`.
- Both wrapper copies and the Surfactant database warmup script passed shell syntax checks. The wrapper now defaults `eyeon parse` to `WARNING` via `EYEON_LOG_LEVEL`/`--log-level` instead of requiring DEBUG-level output for monitor warnings.
- Local five-file large-file serialization smoke test routed all five problematic files through the serial path and completed 5/5, writing JSON for `coder`, `helm`, `k9s`, `parqeye`, and `vcluster`.
- Container-level confirmation still requires rebuilding `peyeon:latest` from the modified `../pEyeON` source.
