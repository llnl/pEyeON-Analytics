---
title: "Component: Container Build and Runtime"
type: component
confidence: high
grounded_by:
  - ../pEyeON/README.md
policy: agent-editable
component: pEyeON-core
last_validated: 2026-06-26
tags: [docker, podman, container, entrypoint]
---

# Component: Container Build and Runtime

## Purpose

EyeON containers package the CLI with extraction dependencies that are not
installed by the simple `pip install peyeon` path, including `ssdeep`,
`libmagic`, `tlsh`, and `detect-it-easy`.

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

<!-- GROUND_TRUTH: ../pEyeON/README.md §local-docker-build -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §local-podman-build -->

## Recommended Runtime Path

For normal field use, users are expected to download `eyeon-parse.sh` and
`eyeon-batch-summary.sh`, pull `ghcr.io/llnl/peyeon:latest`, then run a batch
parse through the wrapper rather than invoking `docker run` directly.

Direct Docker and Podman runs remain useful for development, debugging, and
image validation.

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
