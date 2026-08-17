---
title: "Parse Multiprocessing Hang: Verification"
type: component
confidence: high
grounded_by:
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/tests/testParse.py
policy: agent-editable
last_validated: 2026-08-03
repo_scope: pEyeON
implementation_area: scanner
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/parse-multiprocessing-hang/verification.md
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
- In `../pEyeON`: `uv run python -m py_compile src/eyeon/cli/__init__.py src/eyeon/parse.py src/eyeon/observe.py`
- In `../pEyeON`: `uv run python -m unittest tests.testCli tests.testParse.TestParseFunctions tests.testObserve.GenericMetadataTestCase`
- In both repos: `bash -n eyeon-parse.sh`
- Compared wrapper copies with `cmp -s`; they are identical.

## Results

- Python compile check passed.
- Targeted unit tests passed: 13 tests in `tests.testParse.TestParseFunctions`, `tests.testParse.X86TwoThreadTestCase`, and `tests.testParse.X86ThreeThreadTestCase`.
- Local five-file multiprocessing smoke test completed 5/5 and wrote JSON for `coder`, `helm`, `k9s`, `parqeye`, and `vcluster`.
- Both wrapper copies and the Surfactant database warmup script passed shell syntax checks. The wrapper now defaults `eyeon parse` to `WARNING` via `EYEON_LOG_LEVEL`/`--log-level` instead of requiring DEBUG-level output for monitor warnings.
- Local five-file large-file serialization smoke test routed all five problematic files through the serial path and completed 5/5, writing JSON for `coder`, `helm`, `k9s`, `parqeye`, and `vcluster`.
- Follow-up log-noise fix: CLI configuration now exports `LOGURU_LEVEL` and filters the known Surfactant `FutureWarning: Possible nested set at position 81`; wrappers also pass `LOGURU_LEVEL` and `PYTHONWARNINGS` into containers. Spawned parse workers explicitly configure Loguru from `LOGURU_LEVEL`. Plugin argument dumps and expected generic fallback "no type" messages were downgraded to DEBUG. Targeted CLI/parse/generic metadata tests passed, both wrapper syntax checks passed, and wrapper copies remained identical.
- Container-level confirmation still requires rebuilding `peyeon:latest` from the modified `../pEyeON` source.
