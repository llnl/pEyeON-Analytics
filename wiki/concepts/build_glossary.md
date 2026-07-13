---
title: "Build Glossary (Containers + VM Appliance)"
type: concept
confidence: medium
grounded_by:
  - ../pEyeON/BUILD.md
  - ../pEyeON/builds/Dockerfile
  - ../pEyeON/builds/podman.Dockerfile
  - ../pEyeON/builds/provision/
  - ../pEyeON/builds/vm/build-qcow2.sh
  - ../pEyeON-Analytics/load_eyeon.py
policy: agent-editable
last_validated: 2026-07-10
component: both
tags: [build, glossary, docker, podman, packer, qemu, qcow2, libvirt, uv, dlt, dbt, streamlit]
---

Builder-focused glossary for the build and packaging stack (containers + Debian qcow2 appliance).

## Build Front-Ends

Docker: Container runtime and build tool used for local images and CI.
Repo pointers: `../pEyeON/builds/Dockerfile`
More info: https://docs.docker.com/

Podman: OCI-compatible runtime used for local builds and rootless runs.
Repo pointers: `../pEyeON/builds/podman.Dockerfile`
More info: https://podman.io/

Docker buildx: Docker plugin used for multi-platform builds and multi-arch manifest publishing.
Repo pointers: `../pEyeON/.github/workflows/test-build-container.yaml`, `../pEyeON/.github/workflows/publish-multiarch-container.yaml`
More info: https://docs.docker.com/build/building/multi-platform/

Packer: Image builder that provisions a Debian cloud image into a bootable qcow2 appliance.
Repo pointers: `../pEyeON/builds/vm/build-qcow2.sh`, `../pEyeON/builds/vm/packer/debian12-*.pkr.hcl`
More info: https://developer.hashicorp.com/packer

QEMU: VM emulator used by Packer. On Apple Silicon, arm64 guests can use HVF acceleration; amd64 guests run via TCG emulation.
Repo pointers: `../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl` (CPU model setting)
More info: https://www.qemu.org/

cloud-init: First-boot configuration mechanism used by Debian cloud images and the qcow2 build.
Repo pointers: `../pEyeON/builds/vm/cloud-init/`
More info: https://cloudinit.readthedocs.io/

libvirt / virsh: Linux VM management layer and CLI. Typical workflows are `virsh list`, `virsh console`, `virsh shutdown`.
More info: https://libvirt.org/manpages/virsh.html

## Shared Provisioning Contract

Provision scripts: The build contract is a set of idempotent shell scripts invoked by both Dockerfiles and the Packer templates.
Repo pointers: `../pEyeON/builds/provision/`

apt: Debian package manager used by provisioning scripts to install native dependencies.
More info: https://wiki.debian.org/Apt

uv: Python tool used to install Python versions and sync dependencies from lockfiles (used for analytics provisioning in the VM).
Repo pointers: `../pEyeON/builds/provision/install-uv.sh`, `../pEyeON/builds/provision/install-peyeon-analytics-uv.sh`
More info: https://docs.astral.sh/uv/

## Native Tools Installed in Images

Binwalk v3: Firmware/container analysis and extraction tool compiled as part of image builds.
Repo pointers: `../pEyeON/builds/provision/install-rust-binwalk.sh`
More info: https://github.com/ReFirmLabs/binwalk

sasquatch: SquashFS extractor used by Binwalk v3.
Repo pointers: `../pEyeON/builds/provision/install-sasquatch-deb.sh`
More info: https://github.com/onekey-sec/sasquatch

TLSH: Fuzzy hashing library built from source.
Repo pointers: `../pEyeON/builds/provision/build-tlsh.sh`
More info: https://github.com/trendmicro/tlsh

DuckDB CLI: `duckdb` command-line tool installed for troubleshooting and interactive inspection.
Repo pointers: `../pEyeON/builds/provision/install-duckdb-cli.sh`
More info: https://duckdb.org/docs/api/cli/overview

## Project-Specific Concepts

Appliance VM: A bootable Debian qcow2 image that contains EyeON and the analytics repo. It is not intended to replicate container bind-mount semantics.
Repo pointers: `../pEyeON/builds/vm/`

`eyeon-parse.sh`: Wrapper script used by most users; treats containers as compute and writes results back to a mounted dataset directory.
Repo pointers: `../pEyeON/eyeon-parse.sh`

`load_eyeon.py`: DLT loader that converts EyeON JSON batches into DuckDB bronze/silver tables.
Repo pointers: `../pEyeON-Analytics/load_eyeon.py`

## Pins and Gotchas (What Actually Bit Us)

Apple Silicon + `docker buildx --platform linux/amd64`: Running Rust toolchains under QEMU user-mode can be unstable and may crash (notably when building Binwalk).

Apple Silicon + amd64 qcow2: QEMU TCG emulation is slow; CPU model selection matters for avoiding SIGILL in some Python wheels.

DLT first-run state: `~/.dlt/` is per-user. A fresh user/VM may not have pipeline schemas yet; code should tolerate first-run bootstrap.
Repo pointers: `../pEyeON-Analytics/load_eyeon.py` (`_ensure_destination_tables` behavior)

DuckDB CLI versioning: Defaulting to DuckDB “latest” is convenient, but pin via `DUCKDB_CLI_VERSION` if a regression breaks builds.
