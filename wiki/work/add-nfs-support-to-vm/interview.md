---
title: "Feature Interview: Add NFS Support to VM"
type: concept
confidence: medium
grounded_by:
  - ../pEyeON/builds/provision/configure-dhcp-networkd.sh
  - ../pEyeON/builds/vm/cloud-init/user-data
  - ../pEyeON/builds/README-Deploy.md
policy: agent-editable
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: container
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/add-nfs-support-to-vm/interview.md
tags: [feature-work, vm, nfs, networking, air-gap]
---

# Feature Interview: Add NFS Support to VM

## Initial Idea

Install NFS tools, confirm NFS works, and clean up the VM networking scripts,
while making the networking behavior discoverable to end users.

## Context Established Before Questioning

- The VM is Debian 12 and is built for `amd64` and `arm64` through Packer in
  `../pEyeON/builds/vm/packer/`.
- `../pEyeON/builds/provision/configure-dhcp-networkd.sh` installs an
  `en*` DHCP configuration for `systemd-networkd`.
- `../pEyeON/builds/vm/cloud-init/user-data` already writes `/etc/motd`,
  `/home/eyeon/QUICKSTART.txt`, and an interactive-login quickstart hook.
- The current VM deployment guide describes DHCP but does not explain that the
  VM deliberately uses `systemd-networkd`.

## Interview Log

### Round 1

**Q:** Should NFS be a required VM capability or an optional ingress/egress
workflow? Should the VM be an NFS client, server, or both?

**A:** NFS is optional. The VM should provide client support for mounting scan
inputs and an output landing area. Other file-transfer mechanisms remain valid.

**Outcome:** decision — client-side, optional NFS support; no automatic mount.

### Round 2

**Q:** Should the active DuckDB database be stored on NFS?

**A:** No. Keep the active database on local VM storage and retain a future
warning against putting it on NFS without deliberate locking and consistency
testing.

**Outcome:** decision — exclude the active database from the NFS workflow.

### Round 3

**Q:** How should networking defaults and static networking be handled?

**A:** Keep DHCP as the default, clean up the networking scripts, and provide a
disabled static-network configuration example.

**Outcome:** decision — DHCP default with a shipped static example.

### Round 4

**Q:** How should users discover the non-default networking mechanism and the
optional NFS workflow?

**A:** Use the existing MOTD and local quickstart mechanism. Explain the
`systemd-networkd` choice, provide local networking and NFS discovery/testing
commands, and mention alternative ingress/egress mechanisms.

**Outcome:** decision — improve the existing local login guidance.

### Round 5

**Q:** What runtime constraints apply?

**A:** The VM must work from boot in an air-gapped environment. Required tools,
examples, and guidance must already be installed or present in the image; no
first-boot Internet access may be required.

**Outcome:** constraint — runtime operation must be air-gap capable.

## Decisions

- Install NFS client tooling during image construction.
- Do not embed a deployment-specific NFS server, export, credential, or active
  automatic mount.
- Use NFS for mounted scan inputs and parse-output landing areas.
- Keep the active DuckDB database on local storage.
- Keep DHCP as the default network mode.
- Ship a disabled static-network example.
- Expand the existing `/etc/motd` and `/home/eyeon/QUICKSTART.txt` guidance.

## Constraints

- The resulting VM must operate from boot without Internet access.
- Both VM architectures use the shared provisioning approach.
- NFS discovery commands must account for NFSv4-only servers where RPC-based
  discovery may not work.
- No site-specific network or NFS values may be baked into the image.

## Delegations

- The Engineer/Developer may choose the exact Debian package split and the
  safest shipped filename for disabled examples, provided the defaults remain
  DHCP and no automatic NFS mount is enabled.

## Deferred / Open Questions

- Which deployment environment will provide the primary live NFS verification
  target?
- Whether static networking should be configured solely through a documented
  file example or also exposed through a provisioning variable.
- Whether the local quickstart should include a generic `/etc/fstab` example in
  addition to manual mount commands.

## Playback Summary

The VM will remain DHCP-first and air-gap capable while making its deliberate
`systemd-networkd` configuration visible. It will include optional, client-side
NFS support and local instructions for discovering, testing, mounting, and
unmounting NFS volumes used for scan input and parse output. Static networking
and NFS configuration examples will be shipped disabled. The active DuckDB
database remains local, with a future warning about network-filesystem risks.

## Sealed — human estimates

The Architect supplied durations rather than calendar dates. The answers are
recorded verbatim and were not re-asked.

**Q: If you had to build this exact scope alone, without AI, how many working
hours would it take? And on what date would it realistically have been
available?**
**A: 3 Days**

**Q: With the AI workflow, on what date do you predict this feature will be
available?**
**A: 1 day**
