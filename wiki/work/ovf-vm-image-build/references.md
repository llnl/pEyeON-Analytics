---
title: "OVF/VM Image Build: References"
type: overview
confidence: high
grounded_by:
  - ../pEyeON/builds/Dockerfile
  - ../pEyeON/builds/podman.Dockerfile
  - ../pEyeON/builds/provision/install-build-deps-debian.sh
  - ../pEyeON/builds/provision/install-runtime-deps-debian-docker.sh
  - ../pEyeON/builds/provision/install-runtime-deps-debian-podman.sh
  - ../pEyeON/builds/provision/install-uv.sh
  - ../pEyeON/builds/provision/install-peyeon-analytics-uv.sh
  - ../pEyeON/builds/provision/configure-dhcp-networkd.sh
  - ../pEyeON/builds/provision/install-duckdb-cli.sh
  - ../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl
  - ../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl
  - ../pEyeON/builds/vm/build-qcow2.sh
  - ../pEyeON/README.md
policy: agent-editable
last_validated: 2026-07-09
component: both
tags: [references, container, vm]
---

## Ground Truth Sources

1. `../pEyeON/builds/Dockerfile`
1. `../pEyeON/builds/podman.Dockerfile`
1. Shared provisioning scripts in `../pEyeON/builds/provision/` (install deps, uv/analytics, DHCP config, DuckDB CLI)
1. `../pEyeON/builds/vm/packer/debian12-amd64.pkr.hcl`
1. `../pEyeON/builds/vm/packer/debian12-arm64.pkr.hcl`
1. `../pEyeON/builds/vm/build-qcow2.sh`
1. `../pEyeON/README.md`
