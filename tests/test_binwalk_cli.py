from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from utils import binwalk_cli


class BinwalkCliTests(unittest.TestCase):
    def test_scan_file_parses_findings_and_extractions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            target = tmp_path / "firmware.bin"
            target.write_bytes(b"fixture")
            fake_binwalk = self._fake_binwalk(
                tmp_path,
                returncode=0,
                stderr="missing extractor\n",
                extractions={
                    "uimage-id": {"success": True, "extractor": "uimage_built_in", "size": 12},
                    "squashfs-id": {"success": False, "extractor": "", "output_directory": "out"},
                },
            )

            result = binwalk_cli.scan_file(target, binwalk=fake_binwalk, extract=True, output_dir=tmp_path / "out")

        self.assertEqual(result.returncode, 0)
        self.assertIn("missing extractor", result.stderr)
        self.assertEqual([finding.name for finding in result.findings], ["uimage", "squashfs"])
        self.assertEqual(result.findings[1].offset, 0x240000)
        self.assertEqual(len(result.extractions), 2)
        self.assertEqual([item.finding_id for item in result.extraction_failures], ["squashfs-id"])
        self.assertIn("-e", result.command)
        self.assertIn("-C", result.command)

    def test_nonzero_exit_is_preserved_when_json_is_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            target = tmp_path / "firmware.bin"
            target.write_bytes(b"fixture")
            fake_binwalk = self._fake_binwalk(tmp_path, returncode=3)

            result = binwalk_cli.scan_file(target, binwalk=fake_binwalk)

        self.assertEqual(result.returncode, 3)
        self.assertEqual(len(result.findings), 2)

    def test_missing_json_raises_binwalk_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            target = tmp_path / "firmware.bin"
            target.write_bytes(b"fixture")
            fake_binwalk = self._fake_binwalk(tmp_path, write_json=False)

            with self.assertRaises(binwalk_cli.BinwalkError):
                binwalk_cli.scan_file(target, binwalk=fake_binwalk)

    def test_missing_target_file_raises_file_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(FileNotFoundError):
                binwalk_cli.scan_file(Path(tmpdir) / "missing.bin", binwalk="binwalk")

    def test_result_to_dict_includes_extraction_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            target = tmp_path / "firmware.bin"
            target.write_bytes(b"fixture")
            fake_binwalk = self._fake_binwalk(
                tmp_path,
                extractions={"squashfs-id": {"success": False}},
            )

            data = binwalk_cli.result_to_dict(binwalk_cli.scan_file(target, binwalk=fake_binwalk))

        self.assertEqual(data["extraction_failures"][0]["finding_id"], "squashfs-id")

    def _fake_binwalk(
        self,
        tmp_path: Path,
        *,
        returncode: int = 0,
        stderr: str = "",
        write_json: bool = True,
        extractions: dict[str, dict[str, object]] | None = None,
    ) -> Path:
        script = tmp_path / "fake_binwalk.py"
        payload = [
            {
                "Analysis": {
                    "file_path": "firmware.bin",
                    "file_map": [
                        {
                            "offset": 0,
                            "id": "uimage-id",
                            "size": 2337583,
                            "name": "uimage",
                            "confidence": 250,
                            "description": "uImage firmware image",
                            "always_display": False,
                            "extraction_declined": False,
                        },
                        {
                            "offset": 0x240000,
                            "id": "squashfs-id",
                            "size": 3715756,
                            "name": "squashfs",
                            "confidence": 250,
                            "description": "SquashFS file system",
                            "always_display": False,
                            "extraction_declined": False,
                        },
                    ],
                    "extractions": extractions or {},
                }
            }
        ]
        script.write_text(
            textwrap.dedent(
                f"""
                #!/usr/bin/env python3
                import json
                import sys

                args = sys.argv[1:]
                if {write_json!r}:
                    log_path = args[args.index('-l') + 1]
                    with open(log_path, 'w', encoding='utf-8') as fh:
                        json.dump({payload!r}, fh)
                sys.stderr.write({stderr!r})
                sys.exit({returncode!r})
                """
            ).lstrip(),
            encoding="utf-8",
        )
        script.chmod(0o755)
        return script


if __name__ == "__main__":
    unittest.main()
