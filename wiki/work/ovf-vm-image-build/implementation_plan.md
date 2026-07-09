---
title: "OVF/VM Image Build: Implementation Plan"
type: overview
confidence: low
grounded_by:
  - ../pEyeON/builds/Dockerfile
  - ../pEyeON/builds/podman.Dockerfile
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
policy: agent-editable
last_validated: 2026-07-09
component: both
tags: [plan, packer, qcow2, scripts, refactor]
---

Planning only. Actual implementation work will primarily touch `../pEyeON/`.

## Checklist

1. Choose v1 VM base OS (Debian vs Ubuntu LTS). (Done: Debian 12 / bookworm)
1. Refactor `../pEyeON/builds/Dockerfile` and `../pEyeON/builds/podman.Dockerfile`:
   Done: moved major install steps into `builds/provision/*.sh` and pinned container base to `python:3.13-slim-bookworm`.
1. Create provision scripts in `../pEyeON/builds/` (or `../pEyeON/scripts/`) that capture:
   Done: Debian install scripts for build deps, runtime deps, cmake, tlsh, binwalk, sasquatch, EyeON venv install, and surfactant DB warm.
1. Add a Packer template:
   Done (scaffold): `../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl` + cloud-init seed + `builds/vm/build-qcow2.sh` wrapper.
1. Produce `qcow2` output artifact for Nutanix.
   Done: `bash builds/vm/build-qcow2.sh --arm64` and `--amd64` succeeded on macOS (amd64 under emulation).
1. Document VM usage in `../pEyeON/README.md` under “VM Install”.
   Done: added an “Appliance VM Image (Nutanix / qcow2)” section.
1. Ensure reliable networking in the VM across hypervisors.
   Done: provisioned systemd-networkd DHCP config.
1. Include analytics in the VM.
   Done: copied pEyeON-Analytics to `/opt/pEyeON-Analytics` and installed with `uv`.
1. Ensure amd64 VM builds on Apple Silicon are stable.
   Done: set QEMU CPU model for TCG builds.
1. Include DuckDB CLI for troubleshooting.
   Done: installed via shared provision script.
1. Follow-on: add `vmdk` conversion and OVF/OVA packaging.
   Pending.

## Notes / Constraints

1. Current container base is Debian slim; reusing apt-based provisioning scripts will be easiest if the VM base is Debian/Ubuntu.
<!-- GROUND_TRUTH: ../pEyeON/builds/Dockerfile §python:3.13-slim-bookworm -->
1. If a future bootc path is desired, it likely implies a separate OS-image build definition rather than retrofitting the current toolbox container.
