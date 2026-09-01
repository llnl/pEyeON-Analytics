---
title: "Feature Verification: Add NFS Support to VM"
type: concept
confidence: low
grounded_by:
  - ../pEyeON/builds/vm/build-qcow2.sh
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
  - ../pEyeON/builds/vm/cloud-init/user-data
policy: agent-editable
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: container
format_domain: none
audience: developer
status: draft
source_paths: wiki/work/add-nfs-support-to-vm/verification.md
tags: [feature-work, verification, vm, nfs, networking, air-gap]
---

# Feature Verification: Add NFS Support to VM

## Implementation State

The authorized `../pEyeON` changes were already present in the sibling
worktree when this Developer continuation began. No source files were modified
by this continuation. The checks below validate the current implementation on
macOS; Linux image build and runtime validation remain pending.

## Test Commands

```bash
bash -n builds/provision/install-nfs-client-debian.sh && bash -n builds/provision/configure-dhcp-networkd.sh
```

Full output:

```text
[no output]
```

Exit status: `0`

```bash
git diff --check
```

Full output:

```text
[no output]
```

Exit status: `0`

```bash
packer version
```

Full output:

```text
/bin/bash: packer: command not found
```

Exit status: `127`

```bash
ruby -e 'require "yaml"; YAML.load_file(ARGV.fetch(0))' builds/vm/cloud-init/user-data
```

Full output:

```text
[no output]
```

Exit status: `0`

```bash
ruby -e 'text = File.read(ARGV.fetch(0)); abort "active static network file found" if text.include?("path: /etc/systemd/network/") && text.include?("STATIC-NETWORK"); abort "automatic NFS mount found" if text.include?("/etc/fstab"); abort "missing static example" unless text.include?("/home/eyeon/STATIC-NETWORK.network.example"); abort "missing NFS example" unless text.include?("/home/eyeon/NFS-MOUNTS.example"); abort "missing DuckDB warning" unless text.include?("active DuckDB database on NFS")' builds/vm/cloud-init/user-data
```

Full output:

```text
missing DuckDB warning
```

Exit status: `1`

The assertion used a phrase that does not appear verbatim. The follow-up uses
the actual explicit local-storage and NFS-safety wording.

```bash
ruby -e 'text = File.read(ARGV.fetch(0)); abort "active static network file found" if text.include?("path: /etc/systemd/network/") && text.include?("STATIC-NETWORK"); abort "automatic NFS mount found" if text.include?("/etc/fstab"); abort "missing static example" unless text.include?("/home/eyeon/STATIC-NETWORK.network.example"); abort "missing NFS example" unless text.include?("/home/eyeon/NFS-MOUNTS.example"); abort "missing local DuckDB warning" unless text.include?("Keep the active DuckDB database on local VM storage"); abort "missing NFS safety warning" unless text.include?("active database on NFS without deliberate locking/consistency testing")' builds/vm/cloud-init/user-data
```

Full output:

```text
[no output]
```

Exit status: `0`

```bash
ruby -e 'amd64 = File.read("builds/vm/packer/debian12-amd64.pkr.hcl"); arm64 = File.read("builds/vm/packer/debian12-arm64.pkr.hcl"); needle = "sudo bash /tmp/eyeon-provision/install-nfs-client-debian.sh"; abort "amd64 missing NFS installer" unless amd64.include?(needle); abort "arm64 missing NFS installer" unless arm64.include?(needle)'
```

Full output:

```text
[no output]
```

Exit status: `0`

```bash
ruby -e 'text = File.read("builds/provision/install-nfs-client-debian.sh"); abort "nfs-common not installed" unless text.match?(/\bnfs-common\b/); abort "unexpected fstab write" if text.include?("fstab"); abort "unexpected server install" if text.match?(/nfs-kernel-server|nfs-server/)'
```

Full output:

```text
[no output]
```

Exit status: `0`

```bash
for tool in qemu-system-aarch64 qemu-system-x86_64 qemu-img docker podman; do command -v "$tool" || true; done
```

Full output:

```text
[no output]
```

Exit status: `0` (none of the checked tools is installed)

```bash
packer fmt -check builds/vm/packer/debian12-amd64.pkr.hcl builds/vm/packer/debian12-arm64.pkr.hcl
```

Full output:

```text
/bin/bash: packer: command not found
```

Exit status: `127`

## Static Inspection

The following source inspections passed:

- Both Packer templates invoke `install-nfs-client-debian.sh`.
- `install-nfs-client-debian.sh` installs `nfs-common` and does not configure a
  server or automatic mount.
- DHCP remains the only active `systemd-networkd` file created by provisioning:
  `/etc/systemd/network/20-eyeon-dhcp.network` with `DHCP=yes`.
- The static configuration is `/home/eyeon/STATIC-NETWORK.network.example`, not
  a `.network` file under `/etc/systemd/network`.
- The NFS mount commands are in `/home/eyeon/NFS-MOUNTS.example`, use only
  placeholder server/export values, and do not add an `/etc/fstab` entry.
- The MOTD, quickstart, and deployment guide explain the optional NFS workflow
  and warn against locating an active DuckDB database on NFS.

## Deferred Linux Verification

Run on a Linux host with Packer, QEMU/KVM, and a local NFS server:

```bash
bash ../pEyeON/builds/vm/build-qcow2.sh --amd64
bash ../pEyeON/builds/vm/build-qcow2.sh --arm64
```

In each booted image, with Internet access isolated, run:

```bash
networkctl status
ip addr
resolvectl status
nfsstat --version
mount.nfs --version
rpcinfo -p <server>
showmount -e <server>
```

Then mount a known local input export and writable output export, run a small
parse, verify the output batch is written to the output mount, and unmount both
mounts. Record each exact command and full output in this section.

## Deviations-From-Handoff

- None.

## Known Gaps

- Packer is not installed on the macOS development host, so Packer formatting
  and validation were not run.
- QEMU and Docker/Podman are not installed on the macOS development host, so
  no local image boot or container-based substitute is available.
- No Linux image build or booted VM was available.
- No local NFS server was available for discovery, mount, parse-output, or
  unmount validation.
- Air-gapped first-boot behavior remains a Linux runtime check.

## Follow-Ups

- Execute the Deferred Linux Verification commands on the real Linux test host
  and append the complete output.
