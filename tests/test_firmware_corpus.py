from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from utils import firmware_corpus


class FirmwareCorpusTests(unittest.TestCase):
    def test_default_manifest_loads_and_has_expected_entries(self) -> None:
        manifest = firmware_corpus.load_manifest()

        entries = firmware_corpus.iter_entries(manifest)
        entry_ids = {entry["id"] for entry in entries}

        self.assertGreaterEqual(len(entries), 4)
        self.assertIn("openwrt-ath79-carambola2-23-05-5", entry_ids)
        self.assertIn("dvrf-praetorian-repo", entry_ids)
        self.assertIn("dlink-dir-605l-revb-legacy", entry_ids)
        self.assertIn("firmsec-dataset-index", entry_ids)

    def test_subset_filter_and_table_output(self) -> None:
        manifest = firmware_corpus.load_manifest()

        smoke_entries = firmware_corpus.iter_entries(manifest, "binwalk-smoke")
        table = firmware_corpus.format_entries(smoke_entries)

        self.assertIn("openwrt-ath79-carambola2-23-05-5", [entry["id"] for entry in smoke_entries])
        self.assertIn("iotgoat-v1-0-release", [entry["id"] for entry in smoke_entries])
        self.assertIn("ID", table)
        self.assertIn("DOWNLOAD", table)
        self.assertIn("ARTIFACTS", table)
        self.assertIn("openwrt-ath79-carambola2-23-05-5", table)

    def test_demo_subset_includes_downloadable_and_manual_entries(self) -> None:
        manifest = firmware_corpus.load_manifest()

        demo_entries = firmware_corpus.iter_entries(manifest, "demo")
        by_id = {entry["id"]: entry for entry in demo_entries}

        self.assertEqual(len(firmware_corpus.downloadable_artifacts(by_id["openwrt-ath79-carambola2-23-05-5"])), 1)
        self.assertEqual(len(firmware_corpus.downloadable_artifacts(by_id["dvrf-praetorian-repo"])), 1)
        self.assertEqual(len(firmware_corpus.downloadable_artifacts(by_id["dlink-dir-605l-revb-legacy"])), 0)

    def test_entries_with_fetch_status_for_json_listing(self) -> None:
        manifest = firmware_corpus.load_manifest()
        demo_entries = firmware_corpus.iter_entries(manifest, "demo")

        listed = firmware_corpus.entries_with_fetch_status(demo_entries)
        dvrf = next(entry for entry in listed if entry["id"] == "dvrf-praetorian-repo")

        self.assertTrue(dvrf["fetchable"])
        self.assertEqual(dvrf["downloadable_artifact_count"], 1)
        self.assertEqual(dvrf["artifact_count"], 2)
        self.assertEqual(dvrf["downloadable_artifacts"][0]["path"], "Firmware/DVRF_v03.bin")

    def test_invalid_manifest_rejects_duplicate_ids(self) -> None:
        manifest = {
            "version": 1,
            "entries": [self._entry("duplicate"), self._entry("duplicate")],
        }

        with self.assertRaises(firmware_corpus.ManifestError):
            firmware_corpus.validate_manifest(manifest)

    def test_fetch_file_url_and_verify_checksum(self) -> None:
        payload = b"firmware bytes for checksum test"
        expected_sha256 = hashlib.sha256(payload).hexdigest()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source = tmp_path / "source.bin"
            source.write_bytes(payload)
            entry = self._entry(
                "local-direct",
                source_type="direct-download",
                source_url=source.as_uri(),
                filename="fixture.bin",
                sha256=expected_sha256,
            )

            fetched = firmware_corpus.fetch_entry(entry, tmp_path / "cache")

            self.assertEqual(len(fetched), 1)
            self.assertEqual(fetched[0].read_bytes(), payload)
            self.assertEqual(firmware_corpus.verify_checksum(fetched[0], expected_sha256), expected_sha256)

    def test_fetch_git_repo_artifacts_with_local_file_url(self) -> None:
        payload = b"repo firmware bytes"
        expected_sha256 = hashlib.sha256(payload).hexdigest()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source = tmp_path / "repo.bin"
            source.write_bytes(payload)
            entry = self._entry(
                "repo-entry",
                source_type="git-repo",
                filename=None,
                sha256=None,
                artifacts=[
                    self._artifact(
                        "firmware-one",
                        source_url=source.as_uri(),
                        sha256=expected_sha256,
                    ),
                    self._artifact(
                        "license",
                        artifact_type="license-document",
                        path="Firmware/LICENSE.html",
                        filename="LICENSE.html",
                        source_url=source.as_uri(),
                        fetch=False,
                    ),
                ],
            )

            fetched = firmware_corpus.fetch_entry(entry, tmp_path / "cache")

            self.assertEqual(len(fetched), 1)
            self.assertEqual(fetched[0].read_bytes(), payload)
            self.assertEqual(fetched[0].name, "firmware.bin")
            self.assertIn("firmware-one", fetched[0].as_posix())

    def test_fetch_rejects_checksum_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source = tmp_path / "source.bin"
            source.write_bytes(b"unexpected bytes")
            entry = self._entry(
                "bad-checksum",
                source_type="direct-download",
                source_url=source.as_uri(),
                filename="fixture.bin",
                sha256="0" * 64,
            )

            with self.assertRaises(firmware_corpus.ChecksumError):
                firmware_corpus.fetch_entry(entry, tmp_path / "cache")

    def test_fetch_rejects_non_direct_download_entries(self) -> None:
        entry = self._entry("manual", source_type="vendor-page")

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(firmware_corpus.FetchError):
                firmware_corpus.fetch_entry(entry, Path(tmpdir))

    def test_cli_list_json_uses_manifest_path(self) -> None:
        manifest = {"version": 1, "entries": [self._entry("cli-entry")]}

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with redirect_stdout(StringIO()):
                result = firmware_corpus.main(["--manifest", str(manifest_path), "list", "--json"])

        self.assertEqual(result, 0)

    def _entry(self, entry_id: str, **overrides: object) -> dict[str, object]:
        entry: dict[str, object] = {
            "id": entry_id,
            "name": "Test entry",
            "category": "open-source-baseline",
            "source_url": "https://example.test/firmware.bin",
            "source_type": "direct-download",
            "filename": "firmware.bin",
            "license_or_terms": "Test fixture only.",
            "redistributable": "unknown",
            "expected_size": "tiny",
            "sha256": None,
            "expected_binwalk": [],
            "use_cases": ["unit-test"],
            "subsets": ["unit"],
            "notes": "Synthetic test entry.",
        }
        entry.update(overrides)
        return entry

    def _artifact(self, artifact_id: str, **overrides: object) -> dict[str, object]:
        artifact: dict[str, object] = {
            "id": artifact_id,
            "artifact_type": "firmware-binary",
            "path": "Firmware/firmware.bin",
            "filename": "firmware.bin",
            "source_url": "https://example.test/firmware.bin",
            "sha256": None,
            "fetch": True,
            "notes": "Synthetic test artifact.",
        }
        artifact.update(overrides)
        return artifact


if __name__ == "__main__":
    unittest.main()
