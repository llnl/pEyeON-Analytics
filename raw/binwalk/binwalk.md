# Binwalk & Firmware Analysis Corpus

_Conversation summary and reference — June 2026_

<!-- SOURCE: Claude conversation summary -->
<!-- CREATED: 2026-06-26 -->
<!-- GENERATED_BY: Claude -->
<!-- REVIEWED_BY: Johnson -->
<!-- CONFIDENCE: high -->
<!-- NOTES: Human-reviewed summary of Binwalk status, firmware corpus candidates, and references. External claims should still be verified against linked sources before being promoted to high-confidence wiki pages. -->

---

## Binwalk Status

### The Rust Rewrite (v3)

The original Python-based binwalk (maintained under `ReFirmLabs/binwalk`) was completely rewritten in Rust and released as **v3.1.0 in October 2024**. Key improvements:

- Significantly faster scan times
- Far fewer false positives
- Support for more file extractors
- Windows compilation support added
- Installable via `cargo` or Docker; not yet in most distro package managers

The latest tagged release remains v3.1.0 — no v3.2 as of mid-2026, though the repo continues to accept PRs.

### What Happened to the Python v2 Fork?

The community-maintained **OSPG/binwalk** fork (which kept v2 alive) announced its own EOL at 12/12/2025, citing that the original author is actively developing v3 and there's no need to maintain v2 anymore. Users and distro packagers are directed to migrate to v3.

> Note: `apt install binwalk` on Kali/Debian still installs **v2.4.3** (the Python version). For v3, build from source or use Docker.

### Python Compatibility Shim

A `binwalk3` package on PyPI wraps the v3 Rust binary with the familiar v2 Python API, claiming 2–5x faster analysis and 60–80% reduction in false positives with zero code changes.

---

## Firmware Sample Corpus

### Category 1: Purpose-Built Vulnerable Firmware

Best for hands-on exploit development and learning — intentionally vulnerable, no legal gray areas.

| Name | Description | CVEs / Vulns | Where to Get |
|------|-------------|--------------|--------------|
| **DVRF** (Praetorian) | Damn Vulnerable Router Firmware — MIPS/Linksys E1550 image with intentional pwnables (stack BOF, format strings, etc.) | Intentional — not CVE-mapped | [github.com/praetorian-inc/DVRF](https://github.com/praetorian-inc/DVRF) |
| **DVRF** (BloodyOrangeMan fork) | OpenWrt-based extended DVRF with CTF-style challenges mapped directly to real public CVEs | CVE-2020-7982 (opkg checksum bypass), CVE-2019-12272 (command injection), CVE-2018-1160 (Netatalk OOB write + ASLR/PIE) | [github.com/BloodyOrangeMan/DVRF](https://github.com/BloodyOrangeMan/DVRF) |

**Binwalk output preview for DVRF:**
```
0x0       BIN-Header, board ID: 1550, hardware version: 4702
0x20      TRX firmware header, little endian, CRC32: 0x97096BA6
0x3C      gzip compressed data (piggy)
0x192724  Squashfs filesystem, little endian, version 3.0
```

QEMU emulation works without real hardware — see [Praetorian's getting-started guide](https://www.praetorian.com/blog/getting-started-with-damn-vulnerable-router-firmware-dvrf-v01/).

---

### Category 2: Real-World Vendor Firmware with Known Issues

Publicly downloadable from official or legacy vendor servers. Good for demonstrating real-world impact.

| Vendor / Model | Firmware Version | CVE | Vulnerability | Download |
|----------------|-----------------|-----|---------------|----------|
| **D-Link DIR-605L** | v2.13B01 (Rev B) | CVE-2025-46176 | Hardcoded Telnet credentials (`Alphanetworks` user); plaintext password in `/etc/alpha_config/image_sign`; no patch (EOL Nov 2023) | [D-Link legacy files](https://legacyfiles.us.dlink.com/dir-605l/REVB/FIRMWARE/) |
| **D-Link DIR-816L** | v2.06B01 | CVE-2025-46176 | Same hardcoded Telnet credential issue as DIR-605L; EOL, no patch | [D-Link support page](https://support.dlink.com) — search DIR-816L |
| **D-Link DIR-620** | v1.0.3+ | No formal CVE | Hardcoded `anonymous` credentials in `httpd` binary; OS command injection; documented by Kaspersky Securelist | [Kaspersky writeup](https://securelist.com/backdoors-in-d-links-backyard/85530/) has firmware context |
| **Netgear R6200** | v1.0.1.48 and earlier | CVE-2017-5521 | Password disclosure via crafted web request; `passwordrecovered.cgi` token bypass; affects ~25 Netgear models | [Netgear KB / direct ZIP](http://www.downloads.netgear.com/files/GDC/R6200/R6200-V1.0.1.48_1.0.37.zip) |
| **Netgear R6300** | v1.0.2.78 | CVE-2017-5521 | Same password disclosure bug | [Netgear downloads](https://www.netgear.com/support/) — search R6300 firmware archive |
| **TP-Link Archer C50** | v3 ≤180703, v4 ≤250117, v5 ≤200407 | CVE-2025-6982 | Hardcoded DES decryption keys allow decryption of `config.xml` | [TP-Link FAQ/advisory](https://www.tp-link.com/us/support/faq/4538/) |
| **TP-Link Archer C20** | v5 (affected versions) | CVE-2025-6982 | Same hardcoded DES key issue | [TP-Link support](https://www.tp-link.com/us/support/) — search Archer C20 |

**Binwalk discovery workflow for D-Link DIR-605L CVE-2025-46176:**
```bash
binwalk -eM DIR-605L_REVB_FIRMWARE_v2.13B01_BETA.bin
grep -r "Alphanetworks" squashfs-root/
cat squashfs-root/etc/alpha_config/image_sign   # plaintext password here
cat squashfs-root/bin/telnetd.sh                 # hardcoded cred usage
```

---

### Category 3: Open Source / Reference Firmware (Clean Baseline)

Good for establishing "normal" before demonstrating "abnormal."

| Name | Description | Where to Get |
|------|-------------|--------------|
| **OpenWrt** | Well-documented Linux-based router firmware; unencrypted, rich squashfs+kernel structure; multiple architectures (MIPS, ARM, x86) | [downloads.openwrt.org](https://downloads.openwrt.org) |
| **DD-WRT** | Popular open-source router firmware; good variety of target hardware | [dd-wrt.com/support/other-downloads/](https://dd-wrt.com/support/other-downloads/) |

---

### Category 4: Research Datasets (Bulk Corpus)

For large-scale analysis, ML training, or building automated pipelines.

| Dataset | Size | Vendors Covered | CVEs Mapped | Where to Get |
|---------|------|-----------------|-------------|--------------|
| **FirmSecDataset** (NESA Lab / ISSTA 2022) | 11,086 public + 23,050 private images; 35 device types | Netgear, TP-Link, D-Link, Tenda, and more | 429 CVEs across 34,136 images | [github.com/NESA-Lab/FirmSecDataset](https://github.com/NESA-Lab/FirmSecDataset) — provides official vendor download links |
| **iotwizz/iot-firmware-database** | ~1,000 images | Broad commercial IoT | Varies | [github.com/iotwizz/iot-firmware-database](https://github.com/iotwizz/iot-firmware-database) |
| **Karonte dataset** | 49 images | Netgear, TP-Link, D-Link, Tenda | Referenced via NVD/CVE Binary Tool | Referenced in academic papers; images sourced from official vendor pages |
| **Netgear GPL Archive** (Internet Archive) | Large | Netgear | — | [archive.org/details/netgearfirmwaresgpl](https://archive.org/details/netgearfirmwaresgpl) |

---

## Tools & References

| Tool / Resource | Purpose | Link |
|----------------|---------|-------|
| **binwalk v3** | Firmware analysis (Rust) | [github.com/ReFirmLabs/binwalk](https://github.com/ReFirmLabs/binwalk) |
| **binwalk3** (PyPI) | Python v2 API wrapper for v3 binary | [pypi.org/project/binwalk3](https://pypi.org/project/binwalk3/) |
| **QEMU** | Emulate MIPS/ARM firmware without hardware | `apt install qemu-user-static` |
| **Firmware Analysis Toolkit (attify)** | Automated extraction + emulation pipeline | [github.com/attify/firmware-analysis-toolkit](https://github.com/attify/firmware-analysis-toolkit) |
| **CVE Details** | Per-vendor CVE browsing | [cvedetails.com](https://www.cvedetails.com) |
| **NVD** | Official CVE database | [nvd.nist.gov](https://nvd.nist.gov) |
| **Exploit-DB** | PoC exploits tied to CVEs | [exploit-db.com](https://www.exploit-db.com) |

---

## Quick Start: binwalk v3

```bash
# Install via cargo
cargo install binwalk

# Or via Docker
docker build -t binwalk https://github.com/ReFirmLabs/binwalk.git

# Basic scan
binwalk firmware.bin

# Extract everything recursively
binwalk -eM firmware.bin

# Entropy analysis (find encrypted/compressed regions)
binwalk -E firmware.bin

# List all supported signatures
binwalk --list
```

---

_Sources: ReFirmLabs/binwalk GitHub, OSPG/binwalk EOL notice, NESA-Lab/FirmSecDataset (ISSTA 2022), Kaspersky Securelist DIR-620 advisory, GBHackers CVE-2025-46176 writeup, Trustwave SpiderLabs CVE-2017-5521 advisory, TP-Link CVE-2025-6982 statement, Praetorian DVRF blog._