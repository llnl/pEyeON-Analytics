---
title: "OVF/VM Image Build: Design"
type: overview
confidence: medium
grounded_by:
  - ../pEyeON/builds/Dockerfile
  - ../pEyeON/builds/podman.Dockerfile
  - ../pEyeON/builds/provision/install-build-deps-debian.sh
  - ../pEyeON/builds/provision/install-runtime-deps-debian-docker.sh
  - ../pEyeON/builds/provision/install-runtime-deps-debian-podman.sh
  - ../pEyeON/builds/provision/install-uv.sh
  - ../pEyeON/builds/provision/install-peyeon-analytics-uv.sh
  - ../pEyeON/builds/provision/configure-dhcp-networkd.sh
  - ../pEyeON/builds/provision/install-duckdb-cli.sh
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
  - ../pEyeON/README.md
policy: agent-editable
last_validated: 2026-07-09
repo_scope: cross-repo
implementation_area: container
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/ovf-vm-image-build/design.md
tags: [vm, qcow2, packer, nutanix, ahv, kvm, ova, ovf]
---

## Problem Restatement

We want VM images (Nutanix first) that contain a working EyeON CLI and its native tooling dependencies, without duplicating the install logic already embedded in container build files.

Today’s container build uses `python:3.13-slim-bookworm` and does most setup via shared provisioning scripts invoked from Dockerfile `RUN` blocks.
<!-- GROUND_TRUTH: ../pEyeON/builds/Dockerfile §python:3.13-slim-bookworm -->
<!-- GROUND_TRUTH: ../pEyeON/builds/Dockerfile §COPY builds/provision/ -->

## Design Overview

### Artifact matrix

1. v1: `qcow2` appliance image for Nutanix AHV/KVM.
1. v2: add `vmdk` + OVF/OVA packaging.

### Build pipeline (conceptual)

1. Define a single “provisioning contract” as shell scripts in `../pEyeON/`.
1. Container build: call those scripts from `builds/Dockerfile` and `builds/podman.Dockerfile`.
1. VM build: start from a bootable cloud image; run the same scripts via Packer.

This preserves reuse while acknowledging that containers and VMs differ (kernel/boot/init).

## Script Contract (what gets shared)

The shared scripts should be:

1. Idempotent.
1. Parameterized by distro family (Debian/Ubuntu first).
1. Explicit about what it installs (system packages vs Python venv vs toolchain).
1. Callable from both Docker build context and Packer provisioning.

Suggested split (names illustrative):

1. `builds/provision/install-system-deps-debian.sh`: apt packages required at runtime (and any build-only deps if the VM compiles tools).
1. `builds/provision/install-eyeon-venv.sh`: create venv, `pip install .`, warm any caches/dbs if required (e.g., `surfactant plugin update-db --all`).
<!-- GROUND_TRUTH: ../pEyeON/builds/Dockerfile §surfactant plugin update-db -->
1. `builds/provision/install-extractors.sh`: install Binwalk, sasquatch, 7z tooling, etc, matching what the container provides.
<!-- GROUND_TRUTH: ../pEyeON/README.md §Containers include Binwalk, 7-Zip, sasquatch -->

In practice, the contract lives under `../pEyeON/builds/provision/` and is used by both container builds and Packer templates.
<!-- GROUND_TRUTH: ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl §provisioner -->

## VM UX (appliance)

1. Provide a login user.
1. Enable SSH (recommended for appliance usability).
1. EyeON available on PATH.
1. Minimal guidance: where to put input data, where outputs land, and how to export them.

Current alpha stance:

1. Password login is acceptable.
1. Keep the default `debian` user for convenience.
1. The VM includes pEyeON-Analytics in `/opt/pEyeON-Analytics`.

We explicitly do not replicate the container wrapper’s bind-mount and UID/GID behavior.
<!-- GROUND_TRUTH: ../pEyeON/README.md §eyeon-parse.sh mounts /source and /workdir -->

## Nutanix Notes

1. Nutanix AHV consumes `qcow2` cleanly; treat OVA as a follow-on packaging problem.

## Emulation Note (amd64 builds on Apple Silicon)

When building the Debian amd64 qcow2 on an Apple Silicon host (QEMU TCG), a CPU model that exposes the expected instruction set extensions is required to avoid SIGILL when importing common Python wheels.

<!-- GROUND_TRUTH: ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl §qemuargs -->

## OVF/OVA Notes

1. OVF hardware defaults differ by hypervisor; expect separate small templates/config for VMware vs VirtualBox.
1. Keep this VM-specific metadata outside the shared provisioning scripts.
