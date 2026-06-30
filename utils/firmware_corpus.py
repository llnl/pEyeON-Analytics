"""Manifest-driven firmware corpus utility.

The default corpus intentionally stores pointers and checksums, not firmware
images. Network-dependent fetching is opt-in via the CLI or direct function use.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_PATH = REPO_ROOT / "data" / "firmware_corpus" / "manifest.json"
DEFAULT_DEST = REPO_ROOT / ".firmware-corpus"

VALID_CATEGORIES = {
    "vulnerable-demo",
    "vendor-real-world",
    "open-source-baseline",
    "research-dataset",
}
VALID_SOURCE_TYPES = {
    "direct-download",
    "vendor-page",
    "git-repo",
    "archive",
    "dataset-index",
}
VALID_REDISTRIBUTABLE = {"yes", "no", "unknown", "user-must-download"}
VALID_USE_CASES = {"unit-test", "integration-test", "demo", "spike", "research"}
REQUIRED_ENTRY_FIELDS = {
    "id",
    "name",
    "category",
    "source_url",
    "source_type",
    "license_or_terms",
    "redistributable",
    "expected_size",
    "sha256",
    "expected_binwalk",
    "use_cases",
    "notes",
}
REQUIRED_ARTIFACT_FIELDS = {
    "id",
    "artifact_type",
    "path",
    "filename",
    "source_url",
    "sha256",
    "fetch",
    "notes",
}


class CorpusError(Exception):
    """Base exception for firmware corpus utility failures."""


class ManifestError(CorpusError):
    """Raised when the corpus manifest is invalid."""


class ChecksumError(CorpusError):
    """Raised when a checksum does not match the manifest."""


class FetchError(CorpusError):
    """Raised when an entry cannot be fetched automatically."""


def load_manifest(path: Path | str | None = None) -> dict[str, Any]:
    """Load and validate a corpus manifest."""

    manifest_path = Path(path) if path is not None else DEFAULT_MANIFEST_PATH
    with manifest_path.open("r", encoding="utf-8") as fh:
        manifest = json.load(fh)
    validate_manifest(manifest)
    return manifest


def validate_manifest(manifest: dict[str, Any]) -> None:
    """Validate the manifest structure and supported enum values."""

    if manifest.get("version") != 1:
        raise ManifestError("manifest version must be 1")
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ManifestError("manifest entries must be a non-empty list")

    seen_ids: set[str] = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ManifestError(f"entry {index} must be an object")
        missing = sorted(REQUIRED_ENTRY_FIELDS - set(entry))
        if missing:
            raise ManifestError(f"entry {entry.get('id', index)!r} missing fields: {', '.join(missing)}")

        entry_id = entry["id"]
        if not isinstance(entry_id, str) or not entry_id:
            raise ManifestError(f"entry {index} has invalid id")
        if entry_id in seen_ids:
            raise ManifestError(f"duplicate entry id: {entry_id}")
        seen_ids.add(entry_id)

        _validate_enum(entry, "category", VALID_CATEGORIES)
        _validate_enum(entry, "source_type", VALID_SOURCE_TYPES)
        _validate_enum(entry, "redistributable", VALID_REDISTRIBUTABLE)
        _validate_string_list(entry, "expected_binwalk")
        _validate_string_list(entry, "use_cases")
        for use_case in entry["use_cases"]:
            if use_case not in VALID_USE_CASES:
                raise ManifestError(f"entry {entry_id} has invalid use case: {use_case}")
        if "subsets" in entry:
            _validate_string_list(entry, "subsets")
        sha256 = entry.get("sha256")
        if sha256 is not None and (not isinstance(sha256, str) or len(sha256) != 64):
            raise ManifestError(f"entry {entry_id} has invalid sha256")
        artifacts = entry.get("artifacts", [])
        if not isinstance(artifacts, list):
            raise ManifestError(f"entry {entry_id} artifacts must be a list")
        seen_artifacts: set[str] = set()
        for artifact_index, artifact in enumerate(artifacts):
            _validate_artifact(entry_id, artifact_index, artifact, seen_artifacts)


def _validate_artifact(
    entry_id: str,
    artifact_index: int,
    artifact: Any,
    seen_artifacts: set[str],
) -> None:
    if not isinstance(artifact, dict):
        raise ManifestError(f"entry {entry_id} artifact {artifact_index} must be an object")
    missing = sorted(REQUIRED_ARTIFACT_FIELDS - set(artifact))
    if missing:
        raise ManifestError(f"entry {entry_id} artifact {artifact.get('id', artifact_index)!r} missing fields: {', '.join(missing)}")
    artifact_id = artifact["id"]
    if not isinstance(artifact_id, str) or not artifact_id:
        raise ManifestError(f"entry {entry_id} artifact {artifact_index} has invalid id")
    if artifact_id in seen_artifacts:
        raise ManifestError(f"entry {entry_id} has duplicate artifact id: {artifact_id}")
    seen_artifacts.add(artifact_id)
    for field in ("artifact_type", "path", "filename", "source_url", "notes"):
        value = artifact[field]
        if not isinstance(value, str):
            raise ManifestError(f"entry {entry_id} artifact {artifact_id} field {field} must be a string")
    if not isinstance(artifact["fetch"], bool):
        raise ManifestError(f"entry {entry_id} artifact {artifact_id} field fetch must be a boolean")
    sha256 = artifact.get("sha256")
    if sha256 is not None and (not isinstance(sha256, str) or len(sha256) != 64):
        raise ManifestError(f"entry {entry_id} artifact {artifact_id} has invalid sha256")


def _validate_enum(entry: dict[str, Any], field: str, valid_values: set[str]) -> None:
    value = entry[field]
    if value not in valid_values:
        raise ManifestError(f"entry {entry['id']} has invalid {field}: {value}")


def _validate_string_list(entry: dict[str, Any], field: str) -> None:
    value = entry[field]
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ManifestError(f"entry {entry['id']} field {field} must be a list of strings")


def iter_entries(manifest: dict[str, Any], subset: str | None = None) -> list[dict[str, Any]]:
    """Return entries, optionally filtered by subset name."""

    entries = manifest["entries"]
    if subset is None:
        return list(entries)
    return [entry for entry in entries if subset in entry.get("subsets", [])]


def get_entry(manifest: dict[str, Any], entry_id: str) -> dict[str, Any]:
    """Return one manifest entry by stable ID."""

    for entry in manifest["entries"]:
        if entry["id"] == entry_id:
            return entry
    raise KeyError(f"unknown corpus entry: {entry_id}")


def entry_filename(entry: dict[str, Any]) -> str:
    """Resolve the local filename for an entry."""

    if entry.get("filename"):
        return entry["filename"]
    parsed = urlparse(entry["source_url"])
    filename = Path(unquote(parsed.path)).name
    if not filename:
        raise FetchError(f"entry {entry['id']} does not define a filename")
    return filename


def entry_artifacts(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Return explicit or synthesized artifacts for an entry."""

    artifacts = entry.get("artifacts") or []
    if artifacts:
        return list(artifacts)
    if entry["source_type"] == "direct-download":
        return [
            {
                "id": "primary",
                "artifact_type": "firmware-binary",
                "path": entry_filename(entry),
                "filename": entry_filename(entry),
                "source_url": entry["source_url"],
                "sha256": entry.get("sha256"),
                "fetch": True,
                "notes": "Synthesized from direct-download entry fields.",
                "_from_entry": True,
            }
        ]
    return []


def downloadable_artifacts(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Return artifacts that the fetch command will download."""

    return [artifact for artifact in entry_artifacts(entry) if artifact.get("fetch")]


def entry_path(entry: dict[str, Any], dest: Path | str) -> Path:
    """Return the cache path for an entry under a destination directory."""

    artifacts = downloadable_artifacts(entry)
    if len(artifacts) != 1:
        raise FetchError(f"entry {entry['id']} has {len(artifacts)} downloadable artifacts; use artifact_path")
    return artifact_path(entry, artifacts[0], dest)


def artifact_path(entry: dict[str, Any], artifact: dict[str, Any], dest: Path | str) -> Path:
    """Return the cache path for one entry artifact."""

    if artifact.get("_from_entry"):
        return Path(dest) / entry["id"] / artifact["filename"]
    return Path(dest) / entry["id"] / artifact["id"] / artifact["filename"]


def sha256_file(path: Path | str) -> str:
    """Compute a file's SHA-256 digest."""

    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_checksum(path: Path | str, expected_sha256: str | None) -> str:
    """Verify a file against an expected SHA-256 digest when one is present."""

    actual = sha256_file(path)
    if expected_sha256 and actual.lower() != expected_sha256.lower():
        raise ChecksumError(f"sha256 mismatch for {path}: expected {expected_sha256}, got {actual}")
    return actual


def fetch_entry(
    entry: dict[str, Any],
    dest: Path | str = DEFAULT_DEST,
    *,
    force: bool = False,
    timeout: int = 30,
) -> list[Path]:
    """Fetch all downloadable artifacts for an entry into the local corpus cache."""

    artifacts = downloadable_artifacts(entry)
    if not artifacts:
        raise FetchError(f"entry {entry['id']} has no downloadable artifacts")
    return [fetch_artifact(entry, artifact, dest, force=force, timeout=timeout) for artifact in artifacts]


def fetch_artifact(
    entry: dict[str, Any],
    artifact: dict[str, Any],
    dest: Path | str = DEFAULT_DEST,
    *,
    force: bool = False,
    timeout: int = 30,
) -> Path:
    """Fetch one downloadable artifact into the local corpus cache."""

    if not artifact.get("fetch"):
        raise FetchError(f"entry {entry['id']} artifact {artifact['id']} is not marked for fetch")
    target = artifact_path(entry, artifact, dest)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists() and not force:
        verify_checksum(target, artifact.get("sha256"))
        return target

    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as tmp:
        tmp_path = Path(tmp.name)
        try:
            _copy_url(artifact["source_url"], tmp, timeout)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    try:
        verify_checksum(tmp_path, artifact.get("sha256"))
        tmp_path.replace(target)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return target


def _copy_url(source_url: str, target_fh: Any, timeout: int) -> None:
    parsed = urlparse(source_url)
    if parsed.scheme == "file":
        source_path = Path(unquote(parsed.path))
        with source_path.open("rb") as source_fh:
            shutil.copyfileobj(source_fh, target_fh)
        return
    if parsed.scheme not in {"http", "https"}:
        raise FetchError(f"unsupported URL scheme: {parsed.scheme}")
    with urlopen(source_url, timeout=timeout) as response:  # nosec B310 - URL is explicit manifest/user input.
        shutil.copyfileobj(response, target_fh)


def format_entries(entries: list[dict[str, Any]]) -> str:
    """Format entries as a compact table for CLI output."""

    rows = [["ID", "CATEGORY", "SOURCE", "DOWNLOAD", "ARTIFACTS", "SUBSETS", "NAME"]]
    for entry in entries:
        artifacts = entry_artifacts(entry)
        downloads = downloadable_artifacts(entry)
        rows.append(
            [
                entry["id"],
                entry["category"],
                entry["source_type"],
                "yes" if downloads else "no",
                f"{len(downloads)}/{len(artifacts)}",
                ",".join(entry.get("subsets", [])),
                entry["name"],
            ]
        )
    widths = [max(len(row[column]) for row in rows) for column in range(len(rows[0]))]
    return "\n".join(
        "  ".join(value.ljust(widths[column]) for column, value in enumerate(row)).rstrip()
        for row in rows
    )


def entries_with_fetch_status(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return manifest entries with computed fetch status for JSON listing."""

    listed_entries = []
    for entry in entries:
        artifacts = entry_artifacts(entry)
        downloads = downloadable_artifacts(entry)
        listed = dict(entry)
        listed["fetchable"] = bool(downloads)
        listed["downloadable_artifact_count"] = len(downloads)
        listed["artifact_count"] = len(artifacts)
        listed["downloadable_artifacts"] = [_public_artifact(artifact) for artifact in downloads]
        listed_entries.append(listed)
    return listed_entries


def _public_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in artifact.items() if not key.startswith("_")}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List and fetch EyeON firmware corpus entries.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List manifest entries.")
    list_parser.add_argument("--subset", help="Only show entries in this subset.")
    list_parser.add_argument("--json", action="store_true", help="Emit JSON instead of a table.")

    fetch_parser = subparsers.add_parser("fetch", help="Fetch direct-download entries.")
    fetch_group = fetch_parser.add_mutually_exclusive_group(required=True)
    fetch_group.add_argument("--entry", action="append", dest="entry_ids", help="Entry ID to fetch. May be repeated.")
    fetch_group.add_argument("--subset", help="Fetch all direct-download entries in this subset.")
    fetch_parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    fetch_parser.add_argument("--force", action="store_true")
    fetch_parser.add_argument("--timeout", type=int, default=30)

    verify_parser = subparsers.add_parser("verify", help="Verify a cached entry checksum.")
    verify_parser.add_argument("entry_id")
    verify_parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    manifest = load_manifest(args.manifest)

    try:
        if args.command == "list":
            entries = iter_entries(manifest, args.subset)
            if args.json:
                print(json.dumps(entries_with_fetch_status(entries), indent=2, sort_keys=True))
            else:
                print(format_entries(entries))
            return 0

        if args.command == "fetch":
            if args.entry_ids:
                entries = [get_entry(manifest, entry_id) for entry_id in args.entry_ids]
            else:
                entries = iter_entries(manifest, args.subset)
            fetched = []
            for entry in entries:
                artifacts = downloadable_artifacts(entry)
                print(f"ENTRY {entry['id']}: {len(artifacts)} downloadable artifact(s)")
                if not artifacts:
                    print(f"  SKIP source_type={entry['source_type']} reason=no downloadable artifacts")
                    continue
                for artifact in artifacts:
                    target = artifact_path(entry, artifact, args.dest)
                    action = "FETCH" if args.force or not target.exists() else "CACHE"
                    print(f"  {action} {artifact['id']} {artifact['path']} -> {target}")
                    path = fetch_artifact(entry, artifact, args.dest, force=args.force, timeout=args.timeout)
                    digest = verify_checksum(path, artifact.get("sha256"))
                    print(f"  OK {artifact['id']} sha256={digest}")
                    fetched.append(path)
            print(f"DONE fetched={len(fetched)}")
            return 0

        if args.command == "verify":
            entry = get_entry(manifest, args.entry_id)
            artifacts = downloadable_artifacts(entry)
            if not artifacts:
                raise FetchError(f"entry {entry['id']} has no downloadable artifacts")
            for artifact in artifacts:
                path = artifact_path(entry, artifact, args.dest)
                print(f"{artifact['id']} {verify_checksum(path, artifact.get('sha256'))} {path}")
            return 0
    except CorpusError as exc:
        parser.exit(1, f"error: {exc}\n")

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
