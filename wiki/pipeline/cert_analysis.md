---
title: "Pipeline: Certificate Analysis"
type: pipeline
confidence: high
grounded_by:
  - ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml
  - ../pEyeON-Analytics/dbt_eyeon_gold/models/certs/
policy: agent-editable
last_validated: 2026-06-26
repo_scope: pEyeON-Analytics
implementation_area: dbt-gold
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/pipeline/cert_analysis.md
tags: [certs, x509, expiry, key-size, gold]
---

# Pipeline: Certificate Analysis

## Purpose

The certificate analysis pipeline processes X.509 certificates extracted from signed
binaries (primarily Windows PE files with Authenticode signatures) through a
bronze-silver-gold medallion architecture. It provides deduplication, attribute
parsing, and analytical marts for certificate lifecycle management and organizational
analysis.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml §raw_obs__signatures__certs -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/dbt_eyeon_gold/models/certs/ -->

## Data Flow

### Bronze/Silver Layer

Certificates land in the DLT-managed silver table `raw_obs__signatures__certs` as
a nested child of `raw_obs__signatures`, which is itself nested under `raw_obs`.
Each certificate record includes:

**Core Identity:**
- `cert_sha256` — Certificate SHA256 hash (primary deduplication key)
- `serial_number` — Certificate serial number
- `issuer_name` — Issuer distinguished name (DN)
- `subject_name` — Subject distinguished name (DN)
- `issuer_sha256` — SHA256 hash of issuer certificate (for chain reconstruction)

**Validity Period:**
- `issued_on` — Certificate issuance timestamp
- `expires_on` — Certificate expiration timestamp

**Cryptographic Details:**
- `signed_using` — Signature algorithm (e.g., "sha256WithRSAEncryption")
- `rsa_key_size` — RSA key size in bits

**X.509 Extensions:**
- `basic_constraints` — Basic constraints (e.g., "CA=true")
- `key_usage` — Key usage flags
- `ext_key_usage` — Extended key usage
- `certificate_policies` — Certificate policies OIDs

**Alternative Names:**
- `subject_alt_namexx` — Subject alternative name
- `rfc822_name` — RFC 822 email name
- `directory_name` — Directory name
- `cert_type` — Certificate type

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 442-514 -->

### Gold Layer: Staging

`stg_eyeon__sigs_n_certs` joins the nested certificate, signature, and observation
tables to produce one row per observation-certificate occurrence. It pulls in batch
and utility metadata, signature verification status, and core certificate fields.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/dbt_eyeon_gold/models/certs/staging/stg_eyeon__sigs_n_certs.sql -->

### Gold Layer: Intermediate

**`int_eyeon__unique_certificates`** deduplicates by `cert_sha256`, selecting the
most recent observation per certificate using a `row_number()` window partitioned
by certificate hash.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/dbt_eyeon_gold/models/certs/intermediate/int_eyeon__unique_certificates.sql -->

### Gold Layer: Marts

**`dim_certificates`** is the consumer-facing certificate dimension. It parses the
issuer and subject distinguished names into structured fields using the `x509_attr`
macro:

- Subject: country, state, locality, organization, org unit, common name
- Issuer: country, state, locality, organization, org unit, common name
- Derived: `is_ca` boolean (parsed from `basic_constraints`)

<!-- GROUND_TRUTH: ../pEyeON-Analytics/dbt_eyeon_gold/models/certs/marts/dim_certificates.sql -->

**`fct_observation_certificates`** links observations to certificates for drill-through
from analytical views back to specific binaries.

**Analytical Marts:**
- `mart_cert_expiration_years` — Certificate counts bucketed by expiration year
- `mart_cert_issue_years` — Certificate counts bucketed by issue year
- `mart_cert_key_sizes` — Certificate counts by RSA key size
- `mart_cert_locations` — Observation-certificate counts by utility identifier
- `mart_cert_organizations` — Certificate counts by parsed subject organization
- `mart_cert_subject_states` — Certificate counts by parsed subject state
- `mart_cert_feature_summary` — Certificate counts by CA flag and usage fields

<!-- GROUND_TRUTH: ../pEyeON-Analytics/dbt_eyeon_gold/models/certs/_certs__models.yml -->

## Use Cases

**Expiry Management:** `mart_cert_expiration_years` identifies certificates nearing
expiration across the software inventory.

**Key Size Audit:** `mart_cert_key_sizes` surfaces weak keys (e.g., 1024-bit RSA).

**Organizational Analysis:** `mart_cert_organizations` identifies which vendors or
internal teams sign the most binaries.

**Chain Reconstruction:** `issuer_sha256` links to the issuer certificate's
`cert_sha256` for building trust chains.

## Related

- [[wiki/component/observe]] — observation-level certificate extraction
- [[wiki/schema/silver_layer]] — DLT silver table structure
- [[wiki/concept/authenticode]] — Windows PE signing and authentihash
