---
title: "OVF/VM Image Build: Verification"
type: overview
confidence: low
grounded_by:
  - ../pEyeON/builds/vm/build-qcow2.sh
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
  - ../pEyeON/builds/README-Deploy.md
policy: agent-editable
last_validated: 2026-07-17
component: both
tags: [verification, vm, packer, qcow2]
---

Planning only. Populate once implementation begins.

## Expected Checks

1. Boot the VM successfully (serial console or GUI).
1. SSH login works (if enabled).
1. `eyeon --help` works.
1. Run a small local scan inside the VM (a small directory) and confirm JSON output.
1. External deployment instructions remain aligned with the current qcow2 artifact and supported import targets.

## 2026-07-08 (macOS dev)

Container build validation (post-refactor to provision scripts):

```bash
docker build -f builds/Dockerfile -t peyeon-dev:scripted .
docker run --rm peyeon-dev:scripted eyeon --help
docker run --rm peyeon-dev:scripted binwalk --version
docker run --rm peyeon-dev:scripted sasquatch -h
docker run --rm peyeon-dev:scripted /opt/tlsh/bin/tlsh_unittest
```

Results:

1. `docker build` succeeded.
1. `eyeon --help` succeeded.
1. `binwalk --version` succeeded.
1. `sasquatch` help succeeded.
1. `tlsh_unittest` succeeded.

VM build validation:

```bash
# ARM64 (native on Apple Silicon)
bash ../pEyeON/builds/vm/build-qcow2.sh --arm64

# AMD64 (emulated on Apple Silicon; slow)
bash ../pEyeON/builds/vm/build-qcow2.sh --amd64
```

Results:

1. Debian 12 ARM64 appliance VM build succeeded.
1. Debian 12 AMD64 appliance VM build succeeded (via emulation).
1. Output artifacts:
   `../pEyeON/builds/vm/output/debian12-arm64/eyeon-debian12-arm64.qcow2`
   `../pEyeON/builds/vm/output/debian12-amd64/eyeon-debian12-amd64.qcow2`

Notes:

1. Cross-building an `linux/amd64` container image on Apple Silicon currently fails during Rust toolchain execution when building Binwalk (QEMU user-mode segfault). This affects local `docker buildx build --platform linux/amd64 ...` for the Binwalk-enabled container.

## 2026-07-09 (macOS dev)

VM build validation (analytics + network fixes):

```bash
# ARM64 (native on Apple Silicon)
bash ../pEyeON/builds/vm/build-qcow2.sh --arm64

# AMD64 (emulated on Apple Silicon; slow)
bash ../pEyeON/builds/vm/build-qcow2.sh --amd64
```

Results:

1. ARM64 qcow2 build succeeded.
1. AMD64 qcow2 build succeeded.
1. Output artifacts:
   `../pEyeON/builds/vm/output/debian12-arm64/eyeon-debian12-arm64.qcow2`
   `../pEyeON/builds/vm/output/debian12-amd64/eyeon-debian12-amd64.qcow2`

Notes / fixes applied during this iteration:

1. Analytics install: ensure any copied host `.venv` does not break the guest build by recreating `.venv` in the guest.
2. AMD64 emulation stability: QEMU TCG builds require an explicit CPU model to avoid SIGILL in common Python wheels.

<!-- GROUND_TRUTH: ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl §qemuargs -->

## Field Notes (libvirt / RHEL host)

Observed during early libvirt validation:

1. `virsh console` detach sequence: `Ctrl + ]`.
2. If SSH fails with "Too many authentication failures" (too many keys attempted), force password auth with:

```bash
ssh -o PreferredAuthentications=password -o PubkeyAuthentication=no eyeon@<IP>
```

3. If a guest NIC comes up DOWN under default NAT, manual recovery inside the guest is:

```bash
sudo ip link set <iface> up
sudo dhclient -v <iface>
```

The Debian qcow2 appliance provisioning configures DHCP via systemd-networkd to avoid manual steps in normal cases.

4. On RHEL-like hosts, the libguestfs tooling package is typically `libguestfs-tools` (not `guestfs-tools`).

5. One-liner to get the guest IP (works best on RHEL; does not require the guest agent):

```bash
virsh domifaddr eyeon-debian12-amd64 --source arp 2>/dev/null | awk '/ipv4/ {print $4; exit}' | cut -d/ -f1
```

If the VM is attached to the default libvirt NAT network and the ARP lookup is empty, use the MAC address against DHCP leases:

```bash
mac="$(virsh domiflist eyeon-debian12-amd64 | awk '/network/ {print $5; exit}')" \
  && virsh net-dhcp-leases default | awk -v mac="$mac" 'tolower($0) ~ tolower(mac) {print $5}' | cut -d/ -f1
```

## Field Notes (UTM / macOS)

Observed when booting the qcow2 appliance under UTM 4.7.x on macOS:

1. The appliance is a bootable qcow2 disk image; there is no separate ISO or kernel/initrd to provide.
2. On some UTM 4.7.x builds, `Import Existing Drive` may only be presented under the `Emulate` flow, even for Intel Mac + amd64 guest combinations.
3. If UTM reports `bootindex=0 in use`, it has created multiple first-boot devices. Remove any extra kernel/initrd or installer media and leave only the imported qcow2 as the boot source.
