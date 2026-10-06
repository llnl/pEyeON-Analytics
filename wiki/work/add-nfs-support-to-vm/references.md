---
title: "Feature References: Add NFS Support to VM"
type: concept
confidence: high
grounded_by:
  - ../pEyeON/builds/provision/configure-dhcp-networkd.sh
  - ../pEyeON/builds/vm/cloud-init/user-data
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
  - ../pEyeON/builds/README-Deploy.md
  - wiki/work/ovf-vm-image-build/brief.md
policy: agent-editable
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: container
format_domain: none
audience: developer
status: reviewed
source_paths: wiki/work/add-nfs-support-to-vm/references.md
tags: [feature-work, references, vm, nfs, networking]
---

## Live Repo Sources

- `../pEyeON/builds/provision/configure-dhcp-networkd.sh`
- `../pEyeON/builds/vm/cloud-init/user-data`
- `../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl`
- `../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl`
- `../pEyeON/builds/README-Deploy.md`
- `../pEyeON/builds/vm/build-qcow2.sh`

## External Sources

None required for the initial planning phase. Debian package and DuckDB
network-filesystem behavior should be checked against current authoritative
documentation during implementation.

## Related Wiki Pages

- [[wiki/work/ovf-vm-image-build/brief]]
- [[wiki/work/ovf-vm-image-build/design]]
- [[wiki/work/ovf-vm-image-build/verification]]
- [[wiki/concept/build_glossary]]

## Libraries And APIs

- Debian NFS client package/tooling, expected to include `mount.nfs` and
  `nfsstat`; exact package details are implementation-time verification.
- `systemd-networkd` and its `.network` configuration files.

## Notes

The image must contain all runtime tools and local instructions needed for an
air-gapped boot. NFS is a manually activated client workflow, not a required
service or automatic mount. NFSv4-only servers may not support RPC-based export
discovery, so a known server/export mount test must remain documented.
