---
title: "Pipeline: eyeon-parse.sh Wrapper"
type: pipeline
confidence: high
grounded_by:
  - ../pEyeON/README.md
  - ../pEyeON/eyeon-parse.sh
  - eyeon-parse.sh
  - ../pEyeON-Analytics/README.md
policy: agent-editable
component: pEyeON-core
last_validated: 2026-08-03
tags: [wrapper, container, batch, runtime]
---

# Pipeline: eyeon-parse.sh Wrapper

## Purpose

`eyeon-parse.sh` is the recommended field-use wrapper for batch parsing with
the published EyeON container. It treats the container as compute only: the
source directory is mounted read-only at `/source`, the dataset root is mounted
read-write at `/workdir`, and parse output is written directly back to the host.

<!-- GROUND_TRUTH: ../pEyeON/README.md §eyeon-parse.sh -->

## Invocation Forms

The wrapper supports both option and positional forms:

```bash
./eyeon-parse.sh --util-cd UTIL_CD --dir SOURCE --dataset-path DATASET_PATH --threads 8
./eyeon-parse.sh UTIL_CD SOURCE [DATASET_PATH] [THREADS]
```

`THREADS` defaults to `8`. If `DATASET_PATH` is not provided, the wrapper uses
`datasets.dataset_path` from `EyeOnData.toml`; if that is unavailable, it falls
back to `$HOME/data/eyeon`.

The wrapper runs `eyeon parse` with log level `WARNING` by default so monitor
warnings and high-value problems remain visible without DEBUG-level plugin noise.
Use `EYEON_LOG_LEVEL` or `--log-level` to override the parse log level for a run;
`WARN` is accepted as an alias for `WARNING`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §basic-usage -->

## Batch Output

The wrapper creates a timestamped output directory named `<timestamp>_<UTIL_CD>`
under the dataset path, then runs `eyeon parse` inside the container. The normal
quickstart flow is:

```bash
./eyeon-parse.sh TESTSITE /path/to/software "$HOME/data/eyeon"
./eyeon-batch-summary.sh
```

`eyeon-batch-summary.sh` summarizes the newest batch by default, or one or more
explicit batch directories if paths are passed on the command line. The summary
includes total file count, top-level JSON count, and counts by metadata type.

<!-- GROUND_TRUTH: ../pEyeON/README.md §latest-batch-summary -->

In the analytics quickstart, `eyeon-parse.sh` writes the timestamped batch
directory under `datasets.dataset_path` from `EyeOnData.toml`; that batch can
then be loaded through the Streamlit app.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §generate-a-batch-with-eyeon -->

## Image Selection

By default, the wrapper uses the published production image:

```bash
ghcr.io/llnl/peyeon:latest
```

Development and branch validation can override the image with `EYEON_IMAGE`,
using either branch-scoped or commit-scoped dev tags such as
`ghcr.io/llnl/peyeon:dev-<branch>` or `ghcr.io/llnl/peyeon:dev-<sha>`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §container-image-selection -->

## Runtime Selection

The wrapper supports Docker and Podman. Runtime can be selected with either
`EYEON_CONTAINER_RUNTIME=docker|podman` or `--runtime docker|podman`. If neither
is set, the wrapper auto-selects only when exactly one runtime is installed; if
both are present, it stops and asks the user to choose explicitly.

<!-- GROUND_TRUTH: ../pEyeON/README.md §runtime-selection -->

## Interactive Output

For normal interactive terminal runs, the wrapper allocates a container TTY
(`-it` when stdin and stdout are both terminals, or `-t` when stdout alone is a
terminal), passes `TERM`, and sets `PYTHONUNBUFFERED=1`. This keeps
`alive_progress` progress bars and monitor warnings visible while `eyeon parse`
runs inside Docker or Podman. Non-interactive and redirected runs do not force a
TTY. The wrapper's default `WARNING` log level suppresses routine Surfactant
DEBUG output unless explicitly overridden.

<!-- GROUND_TRUTH: ../pEyeON/eyeon-parse.sh §run_container_parse -->
<!-- GROUND_TRUTH: eyeon-parse.sh §run_container_parse -->

## Ownership Behavior

For Docker, the wrapper passes the caller's UID and GID into the container so
generated output remains owned by the host user. When run as root, the wrapper
requires an explicit output owner through `EYEON_OWNER`, `EYEON_UID`/`EYEON_GID`,
or intentional root passthrough via `EYEON_PASSTHROUGH_ROOT=1`.

Podman relies on Podman's default runtime behavior rather than explicit UID/GID
overrides. The README identifies root-run Podman as admin/debug use rather than
the primary tested mode.

<!-- GROUND_TRUTH: ../pEyeON/README.md §ownership-behavior -->

## Debug Mode

`DEBUG=1` or `--debug` opens an interactive shell instead of immediately running
`eyeon parse`. Debug mode prints the resolved wrapper environment, the container
run command, entrypoint/runtime UID and GID information, metadata for key mounts,
and writes the intended parse command to `/tmp/eyeon-debug-command.sh`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §debug-mode -->

## Related

- [[wiki/components/container]] — image contents, build paths, runtime matrix
- [[wiki/components/parse]] — core `eyeon parse` behavior
- [[wiki/pipeline/dlt_load]] — loading parse batches into analytics tables
