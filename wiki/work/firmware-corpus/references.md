---
title: "Feature References: Firmware Corpus"
type: concept
confidence: medium
grounded_by:
  - raw/binwalk/binwalk.md
policy: agent-editable
last_validated: 2026-06-26
repo_scope: cross-repo
implementation_area: scanner
format_domain: firmware
audience: mixed
status: draft
source_paths: wiki/work/firmware-corpus/references.md
tags: [feature-work, references, firmware, corpus]
---

# Feature References: Firmware Corpus

## Internal Notes

- `raw/binwalk/binwalk.md` - seed list of Binwalk status notes, firmware corpus candidates, tools, and quick-start commands.
- [[wiki/work/firmware-corpus/candidates]] - expanded candidate catalog from additional web research.

## Candidate Corpus Categories

| Category | Purpose | Examples From Notes |
| --- | --- | --- |
| Purpose-built vulnerable firmware | Safe hands-on demos and deterministic vulnerability stories | DVRF, BloodyOrangeMan DVRF fork |
| Real-world vendor firmware | Demonstrate realistic firmware structures and known issues | D-Link DIR-605L, D-Link DIR-816L, D-Link DIR-620, Netgear R6200/R6300, TP-Link Archer C50/C20 |
| Open source/reference firmware | Clean baseline for normal vs abnormal comparisons | OpenWrt, DD-WRT |
| Research datasets | Bulk corpus and broader vendor/device coverage | FirmSecDataset, iotwizz firmware database, Karonte dataset, Netgear GPL Archive |

<!-- GROUND_TRUTH: raw/binwalk/binwalk.md §firmware-sample-corpus -->

See [[wiki/work/firmware-corpus/candidates]] for the expanded candidate catalog,
including IoTGoat, Tasmota, OpenIPC, FirmAE, FIRMADYNE, OpenWrt, and additional
bulk/demo sources.

## Candidate URLs From Notes

- IoTGoat: https://github.com/OWASP/IoTGoat
- IoTGoat challenges: https://github.com/OWASP/IoTGoat/wiki/IoTGoat-challenges
- IoTGoat releases: https://github.com/OWASP/IoTGoat/releases
- DVRF: https://github.com/praetorian-inc/DVRF
- BloodyOrangeMan DVRF fork: https://github.com/BloodyOrangeMan/DVRF
- OpenWrt downloads: https://downloads.openwrt.org/
- OpenWrt firmware selector: https://firmware-selector.openwrt.org/
- Tasmota releases: https://github.com/arendst/Tasmota/releases
- Tasmota OTA releases: http://ota.tasmota.com/tasmota/release
- Tasmota ESP32 OTA releases: https://ota.tasmota.com/tasmota32/release
- OpenIPC firmware releases: https://github.com/OpenIPC/firmware/releases
- ESPHome firmware repository: https://github.com/esphome/firmware
- D-Link DIR-605L legacy files: https://legacyfiles.us.dlink.com/dir-605l/REVB/FIRMWARE/
- D-Link support: https://support.dlink.com
- Netgear R6200 direct ZIP noted in raw source: http://www.downloads.netgear.com/files/GDC/R6200/R6200-V1.0.1.48_1.0.37.zip
- Netgear support: https://www.netgear.com/support/
- TP-Link support/advisory: https://www.tp-link.com/us/support/
- DD-WRT downloads: https://dd-wrt.com/support/other-downloads/
- FirmSecDataset: https://github.com/NESA-Lab/FirmSecDataset
- iotwizz firmware database: https://github.com/iotwizz/iot-firmware-database
- FIRMADYNE: https://github.com/firmadyne/firmadyne
- FirmAE: https://github.com/pr0v3rbs/FirmAE
- Netgear GPL Archive: https://archive.org/details/netgearfirmwaresgpl
- Moxa Software & Documentation: https://www.moxa.com/en/support/product-support/software-and-documentation
- Schneider Electric Documentation & Software Downloads: https://www.se.com/us/en/download/
- Siemens SiePortal / Industry Support: https://support.industry.siemens.com/cs/ww/en/ps
- WAGO Software Engineering Tools: https://www.wago.com/us/software
- Beckhoff Download Finder: https://www.beckhoff.com/en-us/support/download-finder/
- HMS Networks General Downloads: https://www.hms-networks.com/support/general-downloads

## Vulnerability And Advisory References

- CVE-2020-7982: https://nvd.nist.gov/vuln/detail/CVE-2020-7982
- CVE-2018-1160: https://nvd.nist.gov/vuln/detail/CVE-2018-1160
- FirmAE CVE index: https://github.com/pr0v3rbs/FirmAE#cves
- OWASP IoTGoat challenge index: https://github.com/OWASP/IoTGoat/wiki/IoTGoat-challenges

## Related Wiki Pages

- [[wiki/work/binwalk-support/brief]]
- [[wiki/work/binwalk-support/spike]]
- [[wiki/component/parse]]
- [[wiki/component/container]]
- [[wiki/pipeline/eyeon_parse_sh]]
- [[wiki/concept/supply_chain_risk]]

## Source Evaluation Checklist

- Is the source an official vendor, open-source project, or research dataset?
- Is direct download automation allowed by the source terms?
- Can the firmware file be redistributed, or must it be downloaded by the user?
- Is the file small enough for unit tests or only suitable for opt-in integration tests?
- Is a checksum available from the source or can one be recorded after first verified fetch?
- Does the sample have known Binwalk output or extractable embedded content?
- Does the sample support a useful EyeON demo story?

## Notes

- Use "firmware fixture", "container fixture", or "corpus entry" for samples; avoid "Binwalk file" because Binwalk is the tool, not the input format.
- External vulnerability claims in raw notes should be verified against primary sources before they are used in high-confidence docs or demos.
