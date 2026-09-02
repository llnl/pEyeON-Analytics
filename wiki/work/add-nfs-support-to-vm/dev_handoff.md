---
title: "Dev Handoff: Add NFS Support to VM"
type: concept
confidence: medium
grounded_by:
  - ../pEyeON/builds/provision/configure-dhcp-networkd.sh
  - ../pEyeON/builds/vm/cloud-init/user-data
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
  - ../pEyeON/builds/README-Deploy.md
  - wiki/work/add-nfs-support-to-vm/brief.md
  - wiki/work/add-nfs-support-to-vm/design.md
policy: agent-editable
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: container
format_domain: none
audience: developer
status: reviewed
source_paths: wiki/work/add-nfs-support-to-vm/dev_handoff.md
tags:
  - feature-work
  - dev-handoff
  - vm
  - nfs
  - networking
  - air-gap
---

# Dev Handoff: Add NFS Support to VM

**Status:** Approved
**Architect Approval:** Approved 2026-09-01 10:20:58 -0700

## Copy/Paste Prompt

```text
As the Developer, implement the feature: add-nfs-support-to-vm.

Use these wiki files as the handoff context:

- wiki/work/add-nfs-support-to-vm/brief.md
- wiki/work/add-nfs-support-to-vm/design.md
- wiki/work/add-nfs-support-to-vm/implementation_plan.md
- wiki/work/add-nfs-support-to-vm/verification.md

The implementation target is the sibling repository ../pEyeON. Before editing,
read AGENTS.md and confirm this handoff is Approved.
```

## Handoff Summary

Add optional, client-side NFS support to the Debian 12 appliance VM. Install
the required NFS client tooling during image construction so the VM can operate
from boot in an air-gapped environment. Do not install or run an NFS server.
Do not configure an automatic mount or embed a server, export, credential, or
other deployment-specific value.

Keep the current DHCP behavior as the default, but clean up and clarify the
networking provisioning path. Ship a disabled static-network example that
cannot be accidentally selected by `systemd-networkd`. Improve the existing
MOTD and `/home/eyeon/QUICKSTART.txt` so users understand that the VM uses
`systemd-networkd`, can inspect/recover networking, and can choose among local
storage, NFS, or other ingress/egress mechanisms.

The optional NFS workflow must cover mounting scan inputs and an output landing
area for `eyeon-parse.sh`. Ship a disabled NFS mount example with placeholders
only. Include local discovery and test commands, while explaining that
`showmount`/RPC discovery may not work against NFSv4-only servers. Keep the
active DuckDB database on local VM storage and add a future warning against
placing it on NFS without deliberate filesystem testing.

## Authorized Change Boundary

The Developer may modify only the following implementation areas:

- `../pEyeON/builds/provision/`
- `../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl`
- `../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl`
- `../pEyeON/builds/vm/cloud-init/user-data`
- `../pEyeON/builds/README-Deploy.md`
- Additional small VM-build files under `../pEyeON/builds/` only when required
  for the disabled examples or local guidance.

The Developer must not modify the observation schema, analytics application,
active DuckDB data, or unrelated files. Do not add a dependency outside the
Debian VM runtime packages required for NFS client operation without raising it
for review.

## Required Behavior

1. Install the Debian NFS client package(s) at image-build time.
2. Ensure the following classes of commands are available from a booted image:
   - NFS client/version and statistics commands.
   - NFS mount and unmount commands.
   - Network inspection and DNS/status commands.
   - Optional RPC/export discovery commands where package support permits.
3. Preserve DHCP as the active default using the existing
   `systemd-networkd`-based approach.
4. Make the DHCP configuration ownership and behavior clear in local guidance.
5. Provide a disabled static `.network` example. It must not be a valid active
   competing file in the directory scanned by `systemd-networkd`.
6. Provide a disabled NFS mount example with placeholder server/export values.
   It must not create an automatic boot-time mount.
7. Update `/etc/motd` and `/home/eyeon/QUICKSTART.txt` through the existing
   cloud-init mechanism with:
   - DHCP/systemd-networkd explanation.
   - `networkctl`, `ip`, and `resolvectl` inspection guidance.
   - Basic network recovery guidance.
   - Static configuration example pointer.
   - Optional NFS discovery, mount, read/write test, and unmount guidance.
   - NFSv4 discovery limitation.
   - Local-storage and alternative transfer-workflow note.
   - Warning that active DuckDB remains local and is not part of the NFS path.
8. Keep all runtime guidance and required tools inside the image; no first-boot
   Internet access or package installation is allowed.

## Implementation Notes

- Both Packer templates currently duplicate the provisioning command sequence;
  prefer the smallest change that keeps both architectures aligned.
- Do not turn the static example into a live `.network` file. A `.example`,
  `.disabled`, or similarly excluded file is acceptable if the shipped location
  and activation instructions are obvious.
- Do not add a real `/etc/fstab` NFS row. A disabled example may be shipped in a
  local quickstart or explicitly excluded example file.
- NFS discovery is diagnostic, not a prerequisite for mounting a known export.
- The primary Nutanix deployment target is unavailable for live testing. Use
  mocked or local deterministic checks for the build/configuration portions and
  record the unavailable live target as a verification gap.

## Acceptance Criteria

- VM build provisioning installs the required NFS client functionality before
  first boot.
- Both Packer templates remain syntactically valid and use the shared behavior.
- DHCP remains the default and is explained at login.
- The static-network example is disabled and cannot override DHCP accidentally.
- The NFS example is disabled and contains no deployment-specific values.
- Local quickstart guidance is sufficient for an air-gapped user to diagnose
  networking and manually test NFS against a local server.
- The active DuckDB database is explicitly local-only in the guidance.
- No Internet access is required after image construction.

## Verification Requirements

Record exact commands and complete output in
`wiki/work/add-nfs-support-to-vm/verification.md`; do not summarize results.

Required checks:

1. Shell syntax checks for changed provisioning scripts.
2. Packer formatting/validation checks where available without building.
3. VM image build for available architecture(s).
4. Boot/runtime checks with Internet access isolated:
   - network service/configuration status;
   - local MOTD and quickstart availability;
   - NFS command availability;
   - no first-boot dependency downloads.
5. Deterministic inspection of the disabled static and NFS examples.
6. If a local NFS server is available, mount an input export and output export,
   run a small parse, verify output, and unmount. If unavailable, document the
   exact gap and retain the commands for the Architect’s live test.

## Deviations

Any deviation from this handoff must be recorded in
`verification.md` under `Deviations-From-Handoff`. If there are none, write
`None`.

## Closeout Instructions

- Update the checklist in `implementation_plan.md` as work completes.
- Put full verification output in `verification.md`.
- Append a concise implementation entry to `wiki/log.md`.
- Do not promote durable facts into canonical pages until implementation
  stabilizes.
