---
title: "Feature Brief: Add NFS Support to VM"
type: concept
confidence: medium
grounded_by:
  - ../pEyeON/builds/provision/configure-dhcp-networkd.sh
  - ../pEyeON/builds/vm/cloud-init/user-data
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
  - ../pEyeON/builds/README-Deploy.md
policy: agent-editable
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: container
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/add-nfs-support-to-vm/brief.md
tags: [feature-work, vm, nfs, networking, air-gap]
---

# Feature Brief: Add NFS Support to VM

## Problem

The appliance VM deliberately uses a `systemd-networkd` DHCP configuration, but
users can mistake it for the default Debian networking mechanism. The VM also
lacks a ready-to-use, documented NFS client workflow for moving scan inputs and
parse outputs in an air-gapped deployment.

## Goals

- Install all required NFS client tools at image-build time.
- Keep the VM usable from boot without Internet access.
- Keep DHCP as the default network configuration.
- Clean up and clarify the networking provisioning scripts.
- Ship a disabled static-network configuration example.
- Make networking behavior visible through the existing MOTD and local
  quickstart.
- Provide optional NFS guidance for scan input and parse-output volumes.
- Ship a disabled NFS mount example without deployment-specific values.
- Provide local commands to discover and test NFS where supported.

## Non-Goals

- Running an NFS server inside the VM.
- Requiring NFS for normal VM operation.
- Automatic mounting of a site-specific NFS export.
- Storing the active DuckDB database on NFS.
- Requiring Internet access at first boot or during normal parsing.
- Replacing other ingress/egress methods such as SCP, SSHFS, or hypervisor
  shared folders.

## User-Facing Behavior

- On login, the user sees that the VM uses `systemd-networkd` and is DHCP-first.
- The local quickstart points to network inspection, recovery, static-network,
  and optional NFS instructions.
- The user can manually mount an NFS input volume and an output landing volume.
- The user can test NFS tooling and mount permissions locally without an
  Internet service.
- The VM continues to work with local storage or another transfer mechanism when
  NFS is unavailable.

## Acceptance Criteria

- The VM image contains the NFS client commands needed by the documented
  workflow before first boot.
- The VM boots and reaches its default DHCP network without Internet access.
- The default networking path is clearly identified as `systemd-networkd`.
- A disabled static-network example is shipped and cannot override DHCP by
  accident.
- A disabled NFS mount example is shipped with no site-specific values.
- Local quickstart guidance explains NFS discovery, manual mounting, testing,
  and unmounting.
- A live NFS test can mount scan input and parse-output locations and produce
  output in the expected landing directory.
- The active DuckDB database remains on local VM storage.
- The documentation warns that an active DuckDB database on NFS is future work
  requiring deliberate filesystem testing.

## Affected Areas

- `../pEyeON/builds/provision/`
- `../pEyeON/builds/vm/packer/`
- `../pEyeON/builds/vm/cloud-init/user-data`
- `../pEyeON/builds/README-Deploy.md`
- `wiki/work/add-nfs-support-to-vm/`

## References

See `references.md`.

## Open Questions

- Which hypervisor and NFS server will be the primary live verification target?
	- This will have to be mocked: the first target is nutanix, which we don't have available for testing.
- Should static networking be configured only through a shipped file example or
  also through a build/provisioning variable?
	- Just an example file, we'll likely never known the details at VM build time.

## Test Plan

- Validate provision scripts and configuration syntax without requiring a live
  NFS server.
- Build the VM image for the available architecture(s).
- Boot with Internet access unavailable.
- Verify DHCP, DNS/status inspection, NFS tool availability, and local quickstart
  visibility.
- Against a local NFS server, discover where supported, mount input/output
  exports, run a small parse, verify output, and unmount.

## Done When

The air-gapped VM boots with DHCP by default, explains its networking choice,
ships safe static and NFS examples, and supports a manually verified optional
NFS input/output workflow without placing the active database on NFS.
