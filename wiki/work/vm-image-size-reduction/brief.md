---
title: "Feature Brief: VM Image Size Reduction"
type: concept
confidence: medium
grounded_by:
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
  - ../pEyeON/builds/provision/install-build-deps-debian.sh
  - ../pEyeON/builds/provision/install-rust-binwalk.sh
  - ../pEyeON/builds/provision/build-tlsh.sh
  - ../pEyeON/builds/provision/install-runtime-deps-debian-podman.sh
  - ../pEyeON/builds/provision/install-peyeon-analytics-uv.sh
policy: agent-editable
last_validated: 2026-07-17
repo_scope: cross-repo
implementation_area: container
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/vm-image-size-reduction/brief.md
tags: [feature-work, vm, qcow2, size, packer, analytics]
---

# Feature Brief: VM Image Size Reduction

## Problem

The current Debian qcow2 appliance image is materially larger than a minimal EyeON runtime image. The size is not explained by X11 or a desktop environment: the build starts from a Debian cloud image and provisions a headless appliance. The likely size drivers are retained build toolchains, Rust/cargo state, build trees, multiple Python environments, and the optional analytics stack.

## Goals

- Estimate a realistic lower bound for a stripped EyeON CLI appliance.
- Estimate the incremental cost of including `pEyeON-Analytics` in the VM.
- Identify the highest-value cleanup steps that reduce qcow2 size without breaking runtime behavior.
- Preserve a redistributable Debian-based appliance workflow.

## Non-Goals

- Remove a GUI stack that is not currently present.
- Redesign the EyeON runtime itself.
- Optimize for build speed at the expense of appliance correctness.

## Working Estimate

- CLI-only appliance: roughly 2 GB to 3 GB qcow2 after cleanup; potentially lower with aggressive sparsification.
- CLI + analytics appliance: roughly 3 GB to 4 GB qcow2 after cleanup.
- Current image size above those ranges is more likely caused by retained build dependencies and caches than by user-facing runtime tools.

## Suspected Major Size Drivers

- Debian build packages (`build-essential`, `clang`, `python3-dev`, `*-dev`).
- Rust toolchain, cargo registry, and intermediate Binwalk build state.
- TLSH source/build tree under `/opt/tlsh`.
- `uv` / Python caches and analytics `.venv`.
- The analytics checkout itself under `/opt/pEyeON-Analytics`.

## Candidate Reduction Tasks

- Purge build-only Debian packages after provisioning completes.
- Remove `/root/.cargo` and `/root/.rustup` after Binwalk is installed.
- Remove source/build trees that are not required at runtime.
- Split the VM into two flavors: CLI-only and CLI+analytics.
- Add a post-build sparsification step (`fstrim`, `virt-sparsify`, or `qemu-img convert`).

## Acceptance Criteria

- A size-reduction plan identifies exact packages and directories to remove.
- A CLI-only qcow2 target is defined separately from the analytics-inclusive appliance, or the project explicitly decides to keep a single flavor.
- The wiki records measured before/after sizes for at least one build iteration.

## Open Questions

- Should CLI-only and CLI+analytics ship as separate artifacts or remain a single image with an install-time toggle?
- Is post-build sparsification acceptable in CI/manual build workflows?
- What minimum runtime tool set is required for EyeON extraction support versus nice-to-have utilities?
