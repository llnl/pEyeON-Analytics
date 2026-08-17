---
title: "Feature Spike: Firmware Corpus"
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
source_paths: wiki/work/firmware-corpus/spike.md
tags: [feature-work, spike, firmware, corpus]
---

# Feature Spike: Firmware Corpus

## Question

Which initial firmware sample is small, legally usable, easy to fetch, and useful
for Binwalk extraction testing?

## Hypothesis

An open-source baseline firmware image, likely from OpenWrt, may be the best
first spike target because it is public, well-documented, and expected to contain
recognizable Linux firmware structures.

## Experiment

TBD.

Candidate experiment:

1. Select one small OpenWrt image or DVRF sample.
2. Fetch it into a temporary local corpus directory.
3. Run Binwalk scan and extraction.
4. Run EyeON parse over the original and extracted files.
5. Record runtime, output size, and deterministic assertions for future tests.

## Prototype Location

TBD. Prototype scripts should not live in `wiki/`. If retained, place them in an
explicit experimental area approved for code artifacts and cite that path here.

## Results

TBD.

## Recommendation

TBD.

## Follow-Ups

TBD.
