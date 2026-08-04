---
title: "Component: Container Build and Runtime"
type: component
confidence: high
grounded_by:
  - ../pEyeON/README.md
  - ../pEyeON/BUILD.md
  - ../pEyeON/builds/README-Deploy.md
  - ../pEyeON/builds/Dockerfile
  - ../pEyeON/builds/podman.Dockerfile
  - ../pEyeON/builds/provision/install-runtime-deps-debian-docker.sh
  - ../pEyeON/builds/provision/install-runtime-deps-debian-podman.sh
  - ../pEyeON/builds/provision/install-duckdb-cli.sh
  - ../pEyeON/builds/provision/warm-surfactant-dbs.sh
  - ../pEyeON/.github/workflows/ci.yaml
  - ../pEyeON/.github/workflows/publish-container.yaml
policy: agent-editable
component: pEyeON-core
last_validated: 2026-08-03
tags: [docker, podman, container, entrypoint]
---

# Component: Container Build and Runtime

## Purpose

EyeON containers package the CLI with extraction dependencies that are not
installed by the simple `pip install peyeon` path, including `ssdeep`,
`libmagic`, `tlsh`, `detect-it-easy`, Binwalk v3, 7-Zip, and `sasquatch`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §containers -->

## Published Image

The primary image is published to GitHub Container Registry as a multi-arch
image at `ghcr.io/llnl/peyeon:latest`. The same tag works on `amd64` and
`arm64`; Docker pulls the matching architecture automatically.

For development validation, the README documents branch-scoped and commit-scoped
tags under the same package:

```bash
ghcr.io/llnl/peyeon:dev-<branch>
ghcr.io/llnl/peyeon:dev-<sha>
```

<!-- GROUND_TRUTH: ../pEyeON/README.md §published-multi-arch-image -->

## Local Builds

The README documents separate local build flows for Docker and Podman:

```bash
docker build -f builds/Dockerfile -t peyeon .
docker run --rm -it -v "$(pwd):/workdir:Z" peyeon /bin/bash

podman build -t peyeon -f builds/podman.Dockerfile .
podman run --rm -it -v "$(pwd):/workdir:rw" peyeon /bin/bash
```

<!-- GROUND_TRUTH: ../pEyeON/BUILD.md §Local Docker Build (Single-Arch) -->
<!-- GROUND_TRUTH: ../pEyeON/BUILD.md §Local Podman Build (Single-Arch) -->

Build details (including native amd64/arm64 CI builds and publish flows) live in `../pEyeON/BUILD.md`.

<!-- GROUND_TRUTH: ../pEyeON/BUILD.md §Multi-Arch / Cross-Platform Container Builds -->

## Binwalk Packaging

Both `builds/Dockerfile` and `builds/podman.Dockerfile` build Binwalk v3 from
the ReFirmLabs `v3.1.0` tag in the builder stage and copy the resulting
`binwalk` binary into the runtime image. Runtime packages include common
extractors used by Binwalk v3, including `7z`, `sasquatch`, `unar`, `zstd`,
`lz4`, `lzop`, `sleuthkit`, `cabextract`, and `device-tree-compiler`.

Container builds are refactored to call a shared provisioning script set under
`builds/provision/`.

<!-- GROUND_TRUTH: ../pEyeON/builds/Dockerfile §COPY builds/provision/ -->

## DuckDB CLI

The container runtime layer installs the DuckDB CLI as `/usr/local/bin/duckdb` to make it easy to inspect the analytics database (and to support simple on-box troubleshooting). By default it pulls the DuckDB project's latest published CLI asset; `DUCKDB_CLI_VERSION` can be set to pin a specific release.

<!-- GROUND_TRUTH: ../pEyeON/builds/provision/install-runtime-deps-debian-docker.sh §install-duckdb-cli.sh -->
<!-- GROUND_TRUTH: ../pEyeON/builds/provision/install-duckdb-cli.sh §Install the DuckDB CLI binary -->
<!-- GROUND_TRUTH: ../pEyeON/builds/provision/install-duckdb-cli.sh §DUCKDB_CLI_VERSION -->

The current CI smoke tests verify `eyeon --help`, `binwalk --version`,
`sasquatch`, and `7z` for Docker (amd64 and arm64) and Podman (amd64) build paths.

## Surfactant Database Warmup

Both Docker and Podman runtime images set `XDG_DATA_HOME=/opt/eyeon/share` and
`XDG_CONFIG_HOME=/opt/eyeon/config` before running
`builds/provision/warm-surfactant-dbs.sh`. The warm script caches Surfactant's
ReadTheDocs-hosted `database_sources.toml` into the installed package's local
`docs/` path, then runs `surfactant plugin update-db --all` and makes the warmed
data/config directories world-readable. This keeps the runtime user and spawned
parse workers pointed at the same image-global Surfactant database cache and
avoids repeated ReadTheDocs source lookups in air-gapped or isolated runs.

<!-- GROUND_TRUTH: ../pEyeON/builds/Dockerfile §XDG_DATA_HOME -->
<!-- GROUND_TRUTH: ../pEyeON/builds/podman.Dockerfile §XDG_DATA_HOME -->
<!-- GROUND_TRUTH: ../pEyeON/builds/provision/warm-surfactant-dbs.sh -->

## Recommended Runtime Path

For normal field use, users are expected to download `eyeon-parse.sh` and
`eyeon-batch-summary.sh`, pull `ghcr.io/llnl/peyeon:latest`, then run a batch
parse through the wrapper rather than invoking `docker run` directly.

Direct Docker and Podman runs remain useful for development, debugging, and
image validation.

For external VM consumers, deployment/import guidance for the qcow2 appliance lives in `../pEyeON/builds/README-Deploy.md`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §quickstart -->

## Runtime Matrix

The README defines the expected ownership behavior by runtime and host mode:

| Runtime | Host mode | Resulting ownership |
| --- | --- | --- |
| Docker | Normal user | Caller UID/GID |
| Docker | Root with `EYEON_OWNER` or `EYEON_UID`/`EYEON_GID` | Requested UID/GID |
| Docker | Root with `EYEON_PASSTHROUGH_ROOT=1` | Root |
| Podman | Normal user | Caller UID/GID via Podman's rootless mapping |
| Podman | Root shell | Runtime-dependent; admin/debug use only |

Docker uses explicit UID/GID handoff into the container. Podman relies on
Podman's default runtime behavior rather than forcing UID/GID overrides.

<!-- GROUND_TRUTH: ../pEyeON/README.md §runtime-matrix -->

## Jupyter Use

The published image can also run Jupyter for demonstration notebooks by exposing
port `8888` and mounting the working directory at `/workdir`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §jupyter-notebook -->

## Related

- [[wiki/pipeline/eyeon_parse_sh]] — user-facing batch wrapper
- [[wiki/components/parse]] — command invoked inside the container
