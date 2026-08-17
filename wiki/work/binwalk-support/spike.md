---
title: "Feature Spike: Binwalk Support"
type: concept
confidence: medium
grounded_by:
  - https://pypi.org/project/binwalk3/
  - https://github.com/ZachFlint/Binwalk3
  - https://github.com/ReFirmLabs/binwalk
  - https://raw.githubusercontent.com/wiki/ReFirmLabs/binwalk/Cargo-Installation.md
  - https://raw.githubusercontent.com/wiki/ReFirmLabs/binwalk/Using-the-Rust-Library.md
  - https://raw.githubusercontent.com/wiki/ReFirmLabs/binwalk/Compile-From-Source.md
  - https://raw.githubusercontent.com/wiki/ReFirmLabs/binwalk/Supported-Signatures.md
policy: agent-editable
last_validated: 2026-06-26
repo_scope: cross-repo
implementation_area: scanner
format_domain: firmware
audience: mixed
status: draft
source_paths: wiki/work/binwalk-support/spike.md
tags: [feature-work, spike, binwalk, firmware]
---

# Feature Spike: Binwalk Support

## Question

Is there an easy Python-facing Binwalk API path that can be installed and smoke-tested on macOS while still matching the eventual Linux deployment direction?

## Hypothesis

The `binwalk3` PyPI package may provide a Python API compatible with Binwalk v2 while delegating actual scanning to the Binwalk v3 Rust binary. If the Rust binary is easy to install on macOS, this could provide a low-friction local development path.

## Experiment

- Reviewed current Binwalk v3 and `binwalk3` Python package documentation.
- Ran an isolated `uv run --with binwalk3` import/API smoke test outside the repository.
- Checked whether `cargo`, `rustc`, or `binwalk` were already on PATH.
- Checked Homebrew formula availability without installing packages.
- Attempted a basic `binwalk.scan(...)` call against a tiny temporary local file without a Binwalk v3 binary installed.
- After Homebrew `binwalk` was installed by the user, fetched the selected OpenWrt corpus fixture to a temp cache and tested direct CLI detection, CLI JSON logging, CLI extraction, and `binwalk3` Python API detection/extraction.

## Prototype Location

No prototype code retained. Temporary smoke tests ran under `/var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode` and wrote only a tiny temporary file outside the repo.

## Results

- `binwalk3` 3.1.3 installs cleanly as a pure Python package with `uv run --with binwalk3`.
- The package imports as `binwalk` and exposes `binwalk.scan` and `binwalk.Modules`.
- The package does not bundle a macOS Binwalk v3 binary. The installed backend searches for bundled Windows binaries first, then `binwalk3`, `binwalk`, or `binwalk.exe` on PATH.
- Without a Rust Binwalk v3 binary, `binwalk.scan(...)` fails with `ModuleException: Scan failed: Binwalk v3 binary not available at: binwalk3`.
- `cargo`, `rustc`, and `binwalk` were not on PATH in the development shell.
- Homebrew is available at `/opt/homebrew/bin/brew` but not on PATH. `brew info binwalk` reports a bottled `binwalk` 3.1.0 formula with `sevenzip` as its required dependency. `brew info rust` reports Rust is available but not installed.
- ReFirmLabs documents `cargo install binwalk`, but also notes Cargo does not install external dependencies. Binwalk's Rust library API is available for Rust projects but is not a Python API.

## macOS Post-Install Results

Commands used after the user installed Homebrew `binwalk`:

```bash
binwalk --version
brew list --versions binwalk sevenzip
/opt/homebrew/bin/uv run python -m utils.firmware_corpus fetch --entry openwrt-ath79-carambola2-23-05-5 --dest /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-binwalk-corpus
/opt/homebrew/bin/uv run python -m utils.firmware_corpus verify openwrt-ath79-carambola2-23-05-5 --dest /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-binwalk-corpus
binwalk openwrt-23.05.5-ath79-generic-8dev_carambola2-squashfs-sysupgrade.bin
binwalk -q -l openwrt-binwalk.json openwrt-23.05.5-ath79-generic-8dev_carambola2-squashfs-sysupgrade.bin
/opt/homebrew/bin/uv run --with binwalk3 python -c "import binwalk; print(binwalk.scan('firmware.bin'))"
```

Observed results:

- `binwalk --version` reported `binwalk 3.1.0`.
- `brew list --versions` reported `binwalk 3.1.0` and `sevenzip 26.02`.
- The OpenWrt fixture fetched successfully and verified against SHA-256 `08c9eb2d6998fd6faf9ee0b1e9415d2aca20d00c2236e99c51bb0a9dff2056fc`.
- Direct CLI detection found two signatures in about 11 ms: `uimage` at offset `0x0` and `squashfs` at offset `0x240000`.
- CLI JSON logging with `-l` produced a useful shape: top-level list, first item `Analysis.file_path`, `Analysis.file_map`, and `Analysis.extractions`.
- `binwalk3` Python detection worked once the Homebrew binary was on PATH. It returned one `Module` with two `Result` objects, including offsets, descriptions, sizes, source file, and module names `uimage` and `squashfs`.
- CLI extraction with `-e` successfully extracted the built-in `uimage` payload to `MIPS_OpenWrt_Linux-5.15.167.bin`.
- CLI extraction failed for `squashfs` because Binwalk tried to execute `sasquatch`, which was not installed. Binwalk still exited with status `0`.
- `binwalk -L | grep -i -E 'squash|uimage|sasquatch'` confirmed `uimage` uses a built-in extractor and `squashfs` requires `sasquatch`.
- Homebrew has `squashfs` and `squashfuse` formulae, but no `sasquatch` formula was found. Installing normal `squashfs-tools` may not satisfy Binwalk's hard-coded `sasquatch` extractor requirement.
- `binwalk3` extraction produced the same partial output but did not surface the missing `sasquatch` failure in `module.errors`; direct CLI JSON did record `Analysis.extractions[<squashfs-id>].success: false`.

## Recommendation

For the first Python integration spike, prefer `binwalk3` as a thin optional Python adapter only if a Binwalk v3 CLI binary is already present or can be installed by the platform package manager. On macOS, the simplest local path appears to be:

```bash
/opt/homebrew/bin/brew install binwalk
/opt/homebrew/bin/uv run --with binwalk3 python -c "import binwalk; print(binwalk.scan('firmware.bin'))"
```

Do not make `binwalk3` a required dependency yet. The Python wrapper works for detection once Binwalk v3 is installed, but it hides important extraction failure detail. For EyeON's first integration, direct CLI invocation with `-l` JSON output is the stronger default because it exposes `file_map` and per-signature extraction success.

For Linux/container deployment, direct Binwalk v3 CLI invocation is likely simpler and less dependent on a small third-party compatibility wrapper. The container should explicitly install or document external extractors such as `sasquatch` if SquashFS extraction is in scope.

## Wrapper Prototype Result

- Implemented a direct CLI wrapper in `utils/binwalk_cli.py`.
- The wrapper runs Binwalk v3 with `-l` JSON logging, parses `Analysis.file_map` into findings, parses `Analysis.extractions` into extraction records, and preserves `stdout`, `stderr`, `returncode`, and the exact command.
- Unit tests in `tests/test_binwalk_cli.py` use a fake Binwalk executable, so default verification does not require Binwalk, firmware, or network access.
- A real smoke test against the OpenWrt fixture confirmed the wrapper captures both successful built-in `uimage` extraction and failed `squashfs` extraction due to missing `sasquatch`.

## Follow-Ups

- Decide whether first Binwalk support is detection-only or detection plus extraction requiring external extractors.
- If SquashFS extraction is required, identify a supported Linux install path for `sasquatch` and decide whether macOS extraction support is best-effort only.
- Promote or port the direct-CLI wrapper into the core scanner repo after deciding the EyeON observation schema shape.
- Keep extraction tests opt-in because extraction can write large output trees and depends on external tools.
