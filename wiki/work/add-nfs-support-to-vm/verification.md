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
worktree when this Developer continuation began. This continuation made two
small authorized fixes after runtime testing: NFS diagnostics in the quickstart
now use `sudo` so Debian's sbin paths are available, and cloud-init write-file
entries no longer name the `eyeon` owner before that user exists. The checks
below validate the current implementation on macOS and Linux.

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

## Linux Verification (2026-09-01)

The Linux host was `spk16.llnl.gov`, an x86_64 RHEL 8 host with 377 GiB RAM,
KVM available at `/dev/kvm`, and `/usr/libexec/qemu-kvm`. The system command
named `packer` was not HashiCorp Packer; it was `/usr/sbin/packer ->
cracklib-packer`. Official HashiCorp Packer v1.16.0 was downloaded to
`/tmp/opencode/packer-test` and used only for this verification. No project
source or dependency files were changed.

```bash
file /usr/sbin/packer; ls -l /usr/sbin/packer; /usr/sbin/packer --help
```

Full output:

```text
/usr/sbin/packer: symbolic link to cracklib-packer
lrwxrwxrwx. 1 root root 15 Nov 26  2018 /usr/sbin/packer -> cracklib-packer
Usage: /usr/sbin/packer dbname
  if dbname is not specified, will use compiled in default of (/usr/share/cracklib/pw_dict).
```

Exit status: `0`

```bash
/tmp/opencode/packer-test/packer version
/tmp/opencode/packer-test/packer fmt -check builds/vm/packer/debian12-amd64.pkr.hcl builds/vm/packer/debian12-arm64.pkr.hcl
checksum=$(curl -fsSL https://cloud.debian.org/images/cloud/bookworm/latest/SHA512SUMS | awk '/debian-12-generic-amd64\\.qcow2$/ {print $1; exit}'); wheel=$(ls -1 dist/*.whl | tail -1); /tmp/opencode/packer-test/packer validate -var "debian_cloud_image_checksum=sha512:$checksum" -var "eyeon_wheel=$wheel" builds/vm/packer/debian12-amd64.pkr.hcl
checksum=$(curl -fsSL https://cloud.debian.org/images/cloud/bookworm/latest/SHA512SUMS | awk '/debian-12-generic-arm64\\.qcow2$/ {print $1; exit}'); wheel=$(ls -1 dist/*.whl | tail -1); /tmp/opencode/packer-test/packer validate -var "debian_cloud_image_checksum=sha512:$checksum" -var "eyeon_wheel=$wheel" builds/vm/packer/debian12-arm64.pkr.hcl
```

Full output:

```text
Packer v1.16.0
[no output]
The configuration is valid.
The configuration is valid.
```

Exit status: `0` for each command.

```bash
PATH="/tmp/opencode/packer-test:$PATH" builds/vm/build-qcow2.sh --amd64
```

Full output (the command ran for 20 minutes 39 seconds):

```text
Packer retrieved and checksum-verified the Debian amd64 cloud image, created the cloud-init seed disk, copied and resized the guest disk, started the VM with KVM, connected over SSH, and uploaded the provisioning inputs. Provisioning then failed during apt package installation:
Err:2 https://deb.debian.org/debian bookworm InRelease
  Certificate verification failed: The certificate is NOT trusted. The certificate issuer is unknown.
Err:3 https://deb.debian.org/debian bookworm-updates InRelease
  Certificate verification failed: The certificate is NOT trusted. The certificate issuer is unknown.
Err:5 https://deb.debian.org/debian bookworm-backports InRelease
  Certificate verification failed: The certificate is NOT trusted. The certificate issuer is unknown.
Err:6 https://deb.debian.org/debian-security bookworm-security InRelease
  Certificate verification failed: The certificate is NOT trusted. The certificate issuer is unknown.
W: Some index files failed to download. They have been ignored, or old ones used instead.
E: Unable to locate package git
E: Unable to locate package make
E: Unable to locate package unzip
E: Unable to locate package build-essential
E: Unable to locate package clang
E: Unable to locate package pkg-config
E: Package 'python3-dev' has no installation candidate
E: Package 'python3-venv' has no installation candidate
E: Unable to locate package libfontconfig1-dev
E: Unable to locate package liblzma-dev
E: Unable to locate package libssl-dev
Provisioning step had errors: Running the cleanup provisioner, if present...
Deleting output directory...
Build 'qemu.debian12' errored after 20 minutes 39 seconds: Script exited with non-zero exit status: 100. Allowed exit codes are: [0]
Builds finished but no artifacts were created.
```

Exit status: `1`.

The initial invocation without the isolated Packer binary used the unrelated
`cracklib-packer` command and returned a misleading success. Its generated
untracked `*.hwm`, `*.pwd`, and `*.pwi` files were removed. The official Packer
run above is the authoritative build result.

## Successful Linux Build and Runtime Verification (2026-09-02)

After the SSL issue was fixed, the corrected amd64 build completed:

```bash
PATH="/tmp/opencode/packer-test:$PATH" builds/vm/build-qcow2.sh --amd64
```

Final output:

```text
Gracefully halting virtual machine...
Converting hard drive...
Build 'qemu.debian12' finished after 13 minutes 11 seconds.
Wait completed after 13 minutes 11 seconds
Builds finished. The artifacts of successful builds are:
--> qemu.debian12: VM files in directory: builds/vm/output/debian12-amd64
Build complete. Output directory: builds/vm/output/debian12-amd64
Disk image: builds/vm/output/debian12-amd64/eyeon-debian12-amd64.qcow2
```

Exit status: `0`.

Artifact inspection:

```bash
qemu-img info builds/vm/output/debian12-amd64/eyeon-debian12-amd64.qcow2
```

Result: valid qcow2, 20 GiB virtual size, 6.14 GiB allocated size, `corrupt:
false`.

The image was booted with `/usr/libexec/qemu-kvm` using restricted user-mode
networking (`restrict=on`) and SSH port forwarding. The following checks passed
from the `eyeon` account:

```text
systemd-networkd: active
networkctl: State: routable; Online state: online; Address: 10.0.2.15 on ens3
nfsstat: 2.6.2
mount.nfs: (linux nfs-utils 2.6.2)
nfsstat executable: yes
mount.nfs executable: yes
showmount executable: yes
QUICKSTART.txt: present
STATIC-NETWORK.network.example: present
NFS-MOUNTS.example: present
active static example under /etc/systemd/network: no
restricted guest curl to https://deb.debian.org/: curl_status=6
```

The test VM was shut down after verification. No local NFS server was
available, so export discovery, input/output mounts, parse-through-NFS, and
unmount validation remain pending.

An additional live test against `spk16.llnl.gov` confirmed the client-side
workflow reaches the deployment NFS service. `showmount -e` returned the
`/data/fusioncuisine/data` export, and an NFSv3 mount reached both `nfsd` and
`mountd` before the server returned `access denied by server while mounting`.
This is an export ACL or deployed-network authorization issue, not an image
build or NFS-client tooling failure. The shipped image is therefore considered
client-ready; final mount, parse, and unmount validation belongs to the target
deployment environment after its export permissions are corrected.

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
- The amd64 image build and restricted boot smoke test passed on 2026-09-02.
- No local NFS server was available for discovery, mount, parse-output, or
  unmount validation.
- Air-gapped first-boot behavior remains a Linux runtime check.
- The earlier Linux build failure was caused by the Debian guest certificate
  trust issue; the Architect resolved that environment problem before the
  successful build above.
- The first successful boot smoke test found that unqualified NFS commands were
  outside the `eyeon` user's PATH and that cloud-init stopped after writing the
  quickstart because its `eyeon` owner did not yet exist. Both issues were fixed
  within the authorized VM-build change boundary and retested.
- The system `packer` name is occupied by `cracklib-packer`; verification used
  an isolated official Packer binary instead.

## Follow-Ups

- Execute the Deferred Linux Verification commands on the real Linux test host
  and append the complete output.
