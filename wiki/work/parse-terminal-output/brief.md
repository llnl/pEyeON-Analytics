---
title: "Future Work: Parse Terminal Output"
type: component
confidence: medium
grounded_by:
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/src/eyeon/cli/__init__.py
  - ../pEyeON/src/eyeon/observe.py
policy: agent-editable
last_validated: 2026-08-04
repo_scope: pEyeON
implementation_area: scanner
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/parse-terminal-output/brief.md
tags: [parse, logging, progress, rich, terminal]
---

# Future Work: Parse Terminal Output

## Goal

Improve `eyeon parse` terminal output so progress remains stable while warnings,
errors, and slow-file diagnostics remain visible and readable during concurrent
parses.

## Current State

`Parse` currently uses `alive_progress` for progress bars and Loguru for log
messages. Multiprocessing with the `spawn` start method means worker processes
need explicit logging configuration. The current mitigation exports `LOGURU_LEVEL`
and configures spawned workers to avoid DEBUG/INFO log noise at normal wrapper
defaults.

## Candidate Direction

Use Rich as the terminal output layer for TTY runs:

- `rich.progress.Progress` for stable progress display.
- `rich.logging.RichHandler` for readable warning/error formatting.
- Optional `rich.live.Live` / `rich.layout.Layout` for a split terminal view with
  a log region and a progress region.
- Plain line-oriented output when stdout/stderr is not a TTY.

The important architectural change is to centralize human-facing output in the
parent process. Worker processes should send structured events over a queue
instead of writing directly to stderr.

## Sketch

Worker processes emit events such as:

- `started`: file path, pid, timestamp.
- `finished`: file path, pid, duration.
- `warning`: file path, message.
- `error`: file path, exception summary.
- `hung`: file path, pid, duration.

The parent process owns terminal rendering and decides whether to show Rich UI,
plain logs, or quiet output.

## Possible CLI Surface

```bash
eyeon parse --progress auto
eyeon parse --progress rich
eyeon parse --progress plain
eyeon parse --progress none
```

Default behavior should be `auto`: Rich only when attached to a TTY; plain logs
for CI, redirected output, and batch log files.

## Risks

- Rich is a new runtime dependency.
- Full split-screen output can be fragile over SSH, CI logs, and some container
  TTY paths.
- If workers continue writing directly to stderr, any UI library will still be
  vulnerable to garbled output.

## Acceptance Criteria

- Progress remains readable during normal wrapper runs.
- Warnings/errors do not corrupt the progress bar.
- Non-TTY output remains simple, grep-friendly, and suitable for CI logs.
- Worker logs are either centralized through the parent or explicitly suppressed
  unless diagnostics are requested.
