---
title: "Firmware Corpus Candidates"
type: concept
confidence: medium
grounded_by:
  - raw/binwalk/binwalk.md
  - https://github.com/OWASP/IoTGoat
  - https://github.com/OWASP/IoTGoat/wiki/IoTGoat-challenges
  - https://github.com/praetorian-inc/DVRF
  - https://github.com/BloodyOrangeMan/DVRF
  - https://downloads.openwrt.org/
  - https://github.com/arendst/Tasmota/releases
  - https://github.com/OpenIPC/firmware/releases
  - https://github.com/esphome/firmware
  - https://github.com/NESA-Lab/FirmSecDataset
  - https://github.com/iotwizz/iot-firmware-database
  - https://github.com/firmadyne/firmadyne
  - https://github.com/pr0v3rbs/FirmAE
  - https://archive.org/details/netgearfirmwaresgpl
  - https://nvd.nist.gov/vuln/detail/CVE-2020-7982
  - https://nvd.nist.gov/vuln/detail/CVE-2018-1160
  - https://www.moxa.com/en/support/product-support/software-and-documentation
  - https://www.se.com/us/en/download/
  - https://www.wago.com/us/software
  - https://www.beckhoff.com/en-us/support/download-finder/
  - https://www.hms-networks.com/support/general-downloads
policy: agent-editable
last_validated: 2026-06-29
repo_scope: cross-repo
implementation_area: scanner
format_domain: firmware
audience: mixed
status: draft
source_paths: wiki/work/firmware-corpus/candidates.md
tags: [feature-work, firmware, corpus, binwalk, testing, vulnerabilities]
---

# Firmware Corpus Candidates

## Purpose

This page expands the Firmware Corpus candidate pool. It is intentionally broader
than the first implementation manifest: some entries are good for default tests,
some are good for Binwalk spikes, some are useful for demos, and some are bulk
indexes for future research or larger show-and-tell workflows.

Do not treat inclusion here as approval to redistribute firmware. Each candidate
still needs source, license/terms, checksum, size, and test-suitability review
before it enters a source-controlled manifest.

## Best First Manifest Seeds

These are the strongest starting candidates for the first manifest because they
are public, useful for demos, or small enough to make early tooling practical.

| Candidate | Category | Why It Matters | Suggested Use | Notes |
| --- | --- | --- | --- | --- |
| OWASP IoTGoat | `vulnerable-demo` | Deliberately insecure OpenWrt-based firmware maintained by OWASP; challenges map to OWASP IoT Top 10. | Demo, Binwalk spike, integration test candidate | Releases provide precompiled firmware assets; MIT-licensed repo, but verify release asset redistribution separately. |
| Tasmota | `open-source-baseline` | Public ESP8266/ESP32 firmware releases with many small `.bin` and `.bin.gz` assets plus SHA-256 hashes. | Unit/fetch/checksum tests, small binary corpus | Excellent for testing manifest/fetch/checksum behavior because assets are small and checksums are published. |
| OpenIPC firmware | `open-source-baseline` | IP-camera firmware releases with many assets and SHA-256 hashes. | Camera firmware demos, bulk-ish open-source sample set | Nightly/latest releases include hundreds of assets; choose stable or pinned tags to avoid moving targets. |
| OpenWrt stable/archived releases | `open-source-baseline` and `vulnerable-demo` | Canonical embedded Linux firmware baseline with many targets and old vulnerable versions. | Clean baseline, CVE-linked demos, Binwalk filesystem extraction | Old OpenWrt 18.06.x/19.07.0 intersects with CVE-2020-7982; use archives carefully. |
| DVRF original | `vulnerable-demo` | Damn Vulnerable Router Firmware for Linksys E1550, with pwnable binaries and firmware folder. | Vulnerable firmware story, Binwalk extraction | Repo is archived/read-only; verify firmware file availability and license before automation. |
| BloodyOrangeMan DVRF fork | `vulnerable-demo` | OpenWrt-based CTF-style firmware with explicit CVE-mapped challenges. | CVE story demos, challenge corpus | Includes CVE-2019-18993, CVE-2019-12272, CVE-2020-7982, CVE-2018-1160 references in README. |

## Deliberately Vulnerable Firmware

| Candidate | Pointers | Security Story | Corpus Fit |
| --- | --- | --- | --- |
| OWASP IoTGoat | https://github.com/OWASP/IoTGoat, https://github.com/OWASP/IoTGoat/releases, https://github.com/OWASP/IoTGoat/wiki/IoTGoat-challenges | Hardcoded credentials, insecure network services, insecure update mechanism, outdated components, multiple XSS, backdoor/diagnostics, and other OWASP IoT Top 10 categories. | Best demo candidate; likely good Binwalk fixture; challenge wiki gives ready-made narratives. |
| DVRF original | https://github.com/praetorian-inc/DVRF | Linksys E1550-focused intentionally vulnerable router firmware with pwnable source/binaries and QEMU workflow. | Good historical vulnerable firmware; archived repo may be stable. |
| BloodyOrangeMan DVRF | https://github.com/BloodyOrangeMan/DVRF | CTF-style OpenWrt firmware with challenges mapped to CVEs, including CVE-2020-7982 and CVE-2018-1160. | Strong CVE-linked demo candidate; non-English README but technical mapping is explicit. |

## Clean And Open Baselines

| Candidate | Pointers | Why It Matters | Corpus Fit |
| --- | --- | --- | --- |
| OpenWrt current stable | https://downloads.openwrt.org/, https://firmware-selector.openwrt.org/ | Official embedded Linux images across many targets. | Clean baseline and target diversity; use pinned release URLs rather than moving stable aliases. |
| OpenWrt archive | https://downloads.openwrt.org/ archive links | Historic images, including versions relevant to old advisories. | Useful for CVE-linked demos; verify exact version and target. |
| Tasmota | https://github.com/arendst/Tasmota/releases, http://ota.tasmota.com/tasmota/release, https://ota.tasmota.com/tasmota32/release | Small IoT firmware binaries with published checksums and many architectures/builds. | Very strong for fetch/list/checksum implementation tests; less useful for Binwalk filesystem extraction than router images. |
| OpenIPC | https://github.com/OpenIPC/firmware/releases | Open firmware for IP cameras with large release asset sets and SHA-256 hashes. | Good camera/embedded-Linux corpus; choose pinned release assets. |
| ESPHome firmware configs | https://github.com/esphome/firmware | Source/config repository for ESPHome-provided firmware projects. | Useful reference; may be less direct as binary corpus unless release assets are selected. |

## Bulk Corpus And Index Sources

| Candidate | Pointers | Scale | Corpus Fit |
| --- | --- | --- | --- |
| iotwizz IoT firmware database | https://github.com/iotwizz/iot-firmware-database | Claims 1000 commercial IoT firmware entries across 22 categories and 50+ vendors. | Excellent bulk index for demos and future expansion; verify individual links before fetch automation. |
| FirmSecDataset | https://github.com/NESA-Lab/FirmSecDataset | 11,086 public firmware images plus private/desensitized data; focused on third-party component vulnerabilities. | Research-scale index; not default test material. Useful for bulk analysis planning. |
| FIRMADYNE | https://github.com/firmadyne/firmadyne | Evaluated 23,035 images; extracted 9,486; reported 846/1,971 emulated images vulnerable to at least one exploit in paper. | Strong research context and example firmware workflows; not a simple corpus download. |
| FirmAE | https://github.com/pr0v3rbs/FirmAE | Tested 1,124 router/IP-camera firmware images; reports improved emulation and 12 new 0-days affecting 23 devices. | Good source for demo candidates and CVE-linked examples; includes example D-Link firmware URL. |
| Netgear GPL Archive | https://archive.org/details/netgearfirmwaresgpl | Internet Archive item lists ~1.0T of Netgear GPL/source archives from Netgear download paths. | Bulk source-code corpus; useful for dependency/source analysis, less direct than runtime firmware images. |
| DD-WRT downloads | https://dd-wrt.com/support/other-downloads/ | Broad router firmware archive. | Candidate baseline/alternate firmware source; site presented bot protection during review, so automation feasibility is unknown. |

## Real-World Vulnerability Story Candidates

These are not necessarily first tests. They are useful for demos if source terms,
download paths, and sample size work out.

| Candidate | Firmware Pointer | Vulnerability Reference | Story |
| --- | --- | --- | --- |
| OpenWrt 18.06.0-18.06.6 / 19.07.0 | OpenWrt archive via https://downloads.openwrt.org/ | https://nvd.nist.gov/vuln/detail/CVE-2020-7982 | opkg checksum parsing bug could allow malicious package payload installation without verification. Good insecure-update story. |
| BloodyOrangeMan DVRF L6 | https://github.com/BloodyOrangeMan/DVRF | https://nvd.nist.gov/vuln/detail/CVE-2020-7982 | Deliberate challenge based on OpenWrt opkg verification issue. |
| BloodyOrangeMan DVRF L7 | https://github.com/BloodyOrangeMan/DVRF | https://nvd.nist.gov/vuln/detail/CVE-2018-1160 | Deliberate challenge based on Netatalk out-of-bounds write / RCE. |
| FirmAE D-Link examples | https://github.com/pr0v3rbs/FirmAE | FirmAE README links CVE-2018-20114, CVE-2018-19986 through CVE-2018-19990, CVE-2019-6258, CVE-2019-20084. | D-Link router/IP-camera vulnerability story candidates; use FirmAE references as index, then verify exact product/firmware pair. |
| FirmAE ASUS/TRENDNet examples | https://github.com/pr0v3rbs/FirmAE | FirmAE README links CVE-2019-20082, CVE-2019-11399, CVE-2019-11400. | Additional documented vendor vulnerability stories. |
| D-Link DIR-605L / DIR-816L | `raw/binwalk/binwalk.md` seed pointers | Raw notes mention CVE-2025-46176 and hardcoded Telnet credentials. | Potential story candidate, but needs primary-source verification before promotion. |
| Netgear R6200/R6300 | `raw/binwalk/binwalk.md` seed pointers | Raw notes mention CVE-2017-5521 password disclosure. | Potential story candidate, but needs primary-source verification before promotion. |
| TP-Link Archer C50/C20 | `raw/binwalk/binwalk.md` seed pointers | Raw notes mention CVE-2025-6982 and hardcoded DES decryption keys. | Potential story candidate, but needs primary-source verification before promotion. |

## Industrial / OT-Adjacent Expansion Targets

The bulk index source `iotwizz/iot-firmware-database` lists categories that are
useful for EyeON's OT/ICS supply-chain framing. These should be treated as index
categories until individual firmware links are verified.

| Category | Example Vendors Listed By Source | Why It Helps EyeON Demos |
| --- | --- | --- |
| Industrial IoT / SCADA | Moxa, Siemens, Advantech, Weintek | Aligns directly with operational technology inventory and supply-chain narratives. |
| Firewalls / UTM | Fortinet, Zyxel, pfSense, Sophos, MikroTik | Security appliance firmware often has rich binaries and configuration artifacts. |
| Network switches | Netgear, TP-Link, MikroTik, D-Link, Cisco SG | Good for enterprise/utility network inventory demos. |
| Access points | Ubiquiti, TP-Link Omada, Grandstream, EnGenius, Aruba | Common OT/ICS site infrastructure. |
| IP cameras / NVR / DVR | Hikvision, Dahua, Reolink, Uniview, Axis | Strong physical-security/OT-adjacent story and large vendor coverage. |
| Building automation / HVAC | Honeywell, Johnson Controls, Tridium | Direct building/industrial control relevance. |
| Medical IoT | Philips, GE Healthcare | High-consequence supply-chain analysis story, likely opt-in only. |

## Industrial And Utility-Adjacent Vendor Sources

These sources are useful for utility company demos even when no specific cyber
issue is attached. They represent the kinds of rugged networking, automation,
gateway, HMI, and edge devices commonly found around substations, power plants,
water/wastewater facilities, field telemetry networks, and industrial sites.

| Vendor / Source | Public Pointer | Gear Types | Corpus Fit | Automation Notes |
| --- | --- | --- | --- | --- |
| Moxa Software & Documentation | https://www.moxa.com/en/support/product-support/software-and-documentation | Industrial Ethernet switches, secure routers, wireless AP/bridge/client, cellular gateways, serial device servers, protocol gateways, industrial computers, controllers/I/O | Strong utility/OT candidate. Product list includes power, rail, oil and gas, marine, and industrial networking families such as EDS/IKS switches, MGate gateways, NPort serial servers, OnCell cellular, ioLogik/ioPAC I/O, and Moxa Industrial Linux. | Product-series pages may expose downloads by product ID. Good manifest entries may need `vendor-page` or adapter logic rather than direct URLs. |
| Schneider Electric Documentation & Software Downloads | https://www.se.com/us/en/download/ | PLCs, HMIs, drives, UPS/PDU, power monitoring, industrial automation software, energy infrastructure products | Strong electric-utility and facility-power candidate. Useful for power distribution, UPS, and automation inventory stories. | Portal page is public, but product-specific downloads may require filtering or manual selection. Mark early entries as `vendor-page`. |
| Siemens SiePortal / Industry Support | https://support.industry.siemens.com/cs/ww/en/ps | SIMATIC PLC/HMI, SCALANCE industrial networking, drives, automation software, protection/industrial support materials | Strong utility/industrial candidate, especially for automation and industrial networking examples. | Fetched page required JavaScript; use as `manual`/`vendor-page` until download URLs can be verified manually. |
| WAGO Software Engineering Tools | https://www.wago.com/us/software | PLC/controller engineering tools, CODESYS, e!COCKPIT, WAGO controllers and I/O systems, building automation technologies | Good building automation, industrial control, and utility facility candidate. | Page is mainly tooling/software; firmware-specific assets may require product-page follow-up. |
| Beckhoff Download Finder | https://www.beckhoff.com/en-us/support/download-finder/ | TwinCAT, industrial PCs, EtherCAT I/O, automation software, configuration files | Good industrial automation corpus source for software and configuration artifacts. | Download finder is public; some downloads may require myBeckhoff or export-control acknowledgement. Treat as `vendor-page` unless direct URLs are verified. |
| HMS Networks General Downloads | https://www.hms-networks.com/support/general-downloads | Anybus gateways, Ewon remote access, Ixxat CAN interfaces, N-Tron switches, Red Lion HMI/controllers, industrial network security products | Strong OT/utility-adjacent source across protocol converters, remote gateways, industrial switches, HMIs, and CAN/fieldbus tooling. | General downloads expose direct blob URLs for many tools/drivers; product firmware may live on product-specific support pages. Good for manifest adapter exploration. |

## Industrial Sources To Investigate Manually

The following are relevant but were not cleanly fetchable during this research
pass, often due to JavaScript, bot protection, access controls, or uncertain URL
shape. They are still useful targets for a human-curated utility-demo corpus.

| Vendor / Source | Why It Matters | Review Status |
| --- | --- | --- |
| Teltonika Networks | Industrial cellular routers/gateways common in remote telemetry, water/wastewater, and field communications. | Firmware wiki returned HTTP 403 to automated fetch; review manually. |
| AutomationDirect | PLCs, HMIs, drives, industrial Ethernet and field devices common in small industrial/utility deployments. | Support/download fetch failed; review manually. |
| SEL (Schweitzer Engineering Laboratories) | Protection relays, automation controllers, meters, and utility substation gear. | Likely support/download portal with access controls; review manually and handle as `manual` unless public assets exist. |
| Eaton | UPS, power distribution, breakers, meters, industrial controls, and utility/power gear. | Initial firmware-download URL returned 404; locate current portal manually. |
| Phoenix Contact | Industrial networking, PLCnext, I/O, power supplies, and automation hardware. | Initial download URL returned 404; locate current product download pages manually. |
| Digi | Cellular routers, serial servers, embedded modules, and industrial IoT gateways. | Initial firmware URL returned 404; locate current support pages manually. |
| Belden / Hirschmann | Industrial Ethernet switches and OT networking. | Initial download URL returned 404; locate current product download/support pages manually. |

## Utility Demo Story Categories

| Story | Candidate Vendors / Sources | Example Angle |
| --- | --- | --- |
| Substation network inventory | Moxa, Siemens SCALANCE, Hirschmann/Belden, N-Tron/HMS | Rugged switches, routers, and serial gateways in substation networks. |
| Field telemetry and cellular backhaul | Moxa OnCell, Teltonika, Digi, Ewon/HMS | Remote RTU/pump station communications and cellular gateways. |
| Protocol translation and legacy serial | Moxa MGate/NPort, HMS Anybus, Digi serial servers | Modbus/PROFINET/EtherNet/IP/serial bridge inventory. |
| Plant/facility automation | Siemens SIMATIC, Schneider, WAGO, Beckhoff, AutomationDirect | PLC/HMI/controller firmware and automation engineering artifacts. |
| Building and facility systems | WAGO, Schneider, HMS Intesis, Beckhoff | BACnet/KNX/HVAC/building automation gateways. |
| Power and UPS infrastructure | Schneider/APC, Eaton | UPS/PDU/power monitoring firmware and management-card software. |
| Industrial camera and physical security | OpenIPC, Moxa IP cameras/video servers, iotwizz camera categories | Camera/NVR firmware near utility physical-security systems. |

## Suggested Subsets

| Subset | Candidate Entries | Purpose |
| --- | --- | --- |
| `unit-small` | Tasmota `.bin`/`.bin.gz` asset, possibly a tiny Netgear GPL source archive if license allows | Manifest, fetch, checksum, and local file tests. |
| `binwalk-smoke` | IoTGoat firmware, DVRF firmware, one OpenWrt image | Exercise Binwalk detection/extraction over realistic nested firmware. |
| `demo-vulnerable` | IoTGoat, BloodyOrangeMan DVRF, OpenWrt vulnerable version, FirmAE-linked D-Link candidate | Story-driven demos with documented or deliberate security issues. |
| `demo-baseline` | OpenWrt current stable, Tasmota current release, OpenIPC pinned release | Show normal firmware inventory patterns. |
| `demo-utility` | Moxa product-page entries, HMS/Red Lion/Ewon tooling or firmware, Schneider/Siemens/WAGO/Beckhoff manual entries | Utility/industrial site inventory stories even without known CVEs. |
| `bulk-index` | iotwizz, FirmSecDataset, FirmAE, FIRMADYNE, Netgear GPL Archive | Future expansion and larger show-and-tell workflows. |

## Manifest Notes For Dev Agent

- Prefer pinned release/tag URLs over moving `latest` or `nightly` URLs.
- Prefer entries with published SHA-256 hashes for early fetch/checksum logic.
- Mark large/vendor/research entries as `user-must-download`, `manual`, or `opt-in` until terms and automation are clear.
- Keep external-download tests opt-in by default.
- Do not promote vulnerability claims to high confidence unless grounded in NVD, vendor advisories, or project challenge documentation.
