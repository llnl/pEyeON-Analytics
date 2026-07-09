---
title: "OVF/VM Image Build"
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
  - ../pEyeON/builds/vm/build-qcow2.sh
  - ../pEyeON/README.md
policy: agent-editable
last_validated: 2026-07-09
component: both
tags: [vm, ovf, ova, qcow2, packer, nutanix, ahv, kvm, container]
---

## Context

EyeON currently ships and documents a multi-arch container workflow. The current container build is based on `python:3.13-slim-bookworm` and installs a substantial set of native tools and libraries in `builds/Dockerfile`.
<!-- GROUND_TRUTH: ../pEyeON/builds/Dockerfile §python:3.13-slim-bookworm -->

Goal: add VM image outputs (Nutanix first, then VMware/VirtualBox/KVM) while avoiding drift/duplication of install logic across container and VM build paths.

## Decisions (Current)

1. VM style: appliance VM.
1. Do not try to preserve container bind-mount semantics (`/source`, `/workdir`) for the VM.
1. Initial target: Nutanix (AHV/KVM).
1. Constraint: avoid licensing/redistribution friction; images must be freely redistributable.
1. Preference: move installer/config actions out of Dockerfiles into scripts and reuse the scripts for VM provisioning.

## Non-Goals (Initial)

1. No attempt to boot a “container as OS” or reproduce container entrypoint UID/GID mapping inside the VM.
<!-- GROUND_TRUTH: ../pEyeON/builds/entrypoint.sh §UID/GID logic -->
1. No host-directory mount conventions; users choose their own data ingress/egress workflow.

## Proposed Outputs

1. Primary artifact (Nutanix): `qcow2` (upload to Nutanix Image Service).
1. Follow-on artifacts: `vmdk` + OVF/OVA packaging for VMware/VirtualBox.

## Current State (As Built)

1. VM base OS: Debian 12 / bookworm.
1. Outputs: qcow2 for `arm64` and `amd64`.
1. Provisioning reuse: container and VM builds call shared scripts under `../pEyeON/builds/provision/`.
1. Appliance contents include:
   - EyeON CLI + extractors (Binwalk v3, sasquatch, tlsh, etc.)
   - pEyeON-Analytics checkout under `/opt/pEyeON-Analytics` (installed with `uv`)
   - systemd-networkd DHCP config for consistent networking across hypervisors

<!-- GROUND_TRUTH: ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl §provisioner -->
<!-- GROUND_TRUTH: ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl §provisioner -->

## Recommended Approach (Planning)

### One provisioning script set, two build front-ends

1. Refactor container Dockerfiles to call repo scripts instead of inlining large `RUN` blocks.
1. Use Packer (or cloud-image + `virt-customize`) to build a bootable VM disk, and run the same scripts inside the VM build.

Rationale:

1. Today’s container image is a userspace toolbox, not a bootable OS image, so producing a VM requires starting from a bootable base (cloud image) and provisioning it.
<!-- GROUND_TRUTH: ../pEyeON/builds/Dockerfile §toolbox build -->
1. Packer/cloud-image provisioning preserves “no duplication” when the Dockerfiles and VM provisioning both call the same scripts.

### Base OS recommendation (redistribution-friendly)

Prefer a community distro with low redistribution friction:

1. Debian (recommended default) or Ubuntu LTS as the VM base.
1. Fedora/CentOS Stream as alternatives if you later decide to adopt `bootc` OS-image flows.

## Open Questions

1. Packaging scope v1: `qcow2` only, or `qcow2` + `vmdk` + OVF/OVA.

## Clarified Decisions (Alpha)

1. Password login is acceptable.
1. Keep the default `debian` user (alpha/debug convenience).
1. Ship a single VM flavor that includes pEyeON-Analytics.
