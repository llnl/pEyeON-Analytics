---
title: "Implementation Plan: Add NFS Support to VM"
type: concept
confidence: low
grounded_by:
  - ../pEyeON/builds/provision/configure-dhcp-networkd.sh
  - ../pEyeON/builds/vm/cloud-init/user-data
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
policy: agent-editable
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: container
format_domain: none
audience: developer
status: draft
source_paths: wiki/work/add-nfs-support-to-vm/implementation_plan.md
tags: [feature-work, plan, vm, nfs, networking, air-gap]
---

# Implementation Plan: Add NFS Support to VM

## Scope

Implementation is expected primarily in `../pEyeON`; this repository holds the
feature record. No schema or analytics application changes are expected.

## Checklist

- [x] Inspect current Debian runtime package scripts and select required NFS
  client package(s): `nfs-common`.
- [x] Add NFS client installation to the shared VM provisioning path.
- [x] Refactor or clarify the DHCP networking script without changing DHCP as
  the default.
- [x] Add a disabled static-network example that cannot override DHCP.
- [x] Add a disabled NFS mount example with placeholders only.
- [x] Update `/etc/motd` and `/home/eyeon/QUICKSTART.txt` with local networking,
  static-network, NFS, and alternative transfer guidance.
- [x] Add the future warning about keeping the active DuckDB database off NFS.
- [x] Update `../pEyeON/builds/README-Deploy.md`.
- [x] Build the available amd64 VM architecture; arm64 remains unbuilt on this
  host.
- [x] Boot-test the amd64 image in a restricted-network runtime environment.
- [ ] Test DHCP and static-network instructions.
- [ ] Test NFS tool discovery, manual input/output mounts, parsing, and
  unmounting against a local NFS server.
- [x] Record macOS static verification in `verification.md` with exact commands
  and full output; Linux build/runtime verification remains pending.
- [x] Prepare an Architect-approved `dev_handoff.md` before implementation.

## Constraints

- Do not add automatic NFS mounts or site-specific values.
- Do not make Internet access a runtime prerequisite.
- Do not put the active DuckDB database on NFS.
- Do not modify the sibling repository until implementation authorization and an
  approved handoff are available.

## Verification Checklist

- [ ] Package and command availability from a booted, offline VM.
- [ ] DHCP starts using the intended `systemd-networkd` configuration.
- [ ] Login guidance is visible and locally complete.
- [x] Static configuration example is safe and understandable by source
  inspection; booted-VM confirmation remains pending.
- [ ] NFS mount and unmount succeed against the test server.
- [ ] Scan input is read from NFS.
- [ ] Parse output lands on NFS.
- [ ] Local DuckDB behavior remains unchanged.
