"""Direct Binwalk v3 CLI wrapper.

This module intentionally shells out to the Binwalk CLI instead of importing a
Python compatibility package. Binwalk's JSON log includes extraction status that
is useful for diagnosing missing external extractors.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


class BinwalkError(Exception):
    """Raised when Binwalk cannot be executed or its JSON cannot be parsed."""


@dataclass(frozen=True)
class BinwalkFinding:
    """One signature finding from Binwalk's `Analysis.file_map`."""

    offset: int
    id: str | None
    name: str
    description: str
    size: int | None = None
    confidence: int | None = None
    always_display: bool | None = None
    extraction_declined: bool | None = None


@dataclass(frozen=True)
class BinwalkExtraction:
    """One extraction status record from Binwalk's `Analysis.extractions`."""

    finding_id: str
    success: bool
    output_directory: str | None = None
    extractor: str | None = None
    size: int | None = None
    do_not_recurse: bool | None = None


@dataclass(frozen=True)
class BinwalkScanResult:
    """Parsed Binwalk scan result plus process diagnostics."""

    file_path: str
    findings: list[BinwalkFinding]
    extractions: list[BinwalkExtraction]
    returncode: int
    stdout: str
    stderr: str
    command: list[str]
    raw_json: Any

    @property
    def extraction_failures(self) -> list[BinwalkExtraction]:
        """Return extraction records whose `success` flag is false."""

        return [extraction for extraction in self.extractions if not extraction.success]


def scan_file(
    file_path: Path | str,
    *,
    binwalk: Path | str = "binwalk",
    extract: bool = False,
    matryoshka: bool = False,
    output_dir: Path | str | None = None,
    timeout: int = 600,
    quiet: bool = True,
    extra_args: list[str] | None = None,
) -> BinwalkScanResult:
    """Run Binwalk v3 against one file and parse its JSON log.

    The returned result preserves non-zero exit codes instead of raising for
    them. Exceptions are reserved for process launch failures, timeouts, and
    malformed/missing JSON output.
    """

    target = Path(file_path)
    if not target.is_file():
        raise FileNotFoundError(target)

    with tempfile.TemporaryDirectory(prefix="eyeon-binwalk-") as tmpdir:
        json_path = Path(tmpdir) / "binwalk.json"
        command = _build_command(
            binwalk=binwalk,
            json_path=json_path,
            file_path=target,
            extract=extract,
            matryoshka=matryoshka,
            output_dir=output_dir,
            quiet=quiet,
            extra_args=extra_args,
        )
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except FileNotFoundError as exc:
            raise BinwalkError(f"Binwalk executable not found: {binwalk}") from exc
        except subprocess.TimeoutExpired as exc:
            raise BinwalkError(f"Binwalk timed out after {timeout} seconds") from exc

        raw_json = _load_json(json_path)
        return _parse_result(raw_json, completed, command, fallback_file_path=str(target))


def _build_command(
    *,
    binwalk: Path | str,
    json_path: Path,
    file_path: Path,
    extract: bool,
    matryoshka: bool,
    output_dir: Path | str | None,
    quiet: bool,
    extra_args: list[str] | None,
) -> list[str]:
    command = [str(binwalk)]
    if quiet:
        command.append("-q")
    if extract:
        command.append("-e")
    if matryoshka:
        command.append("-M")
    if output_dir is not None:
        command.extend(["-C", str(output_dir)])
    command.extend(["-l", str(json_path)])
    if extra_args:
        command.extend(extra_args)
    command.append(str(file_path))
    return command


def _load_json(json_path: Path) -> Any:
    try:
        with json_path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError as exc:
        raise BinwalkError(f"Binwalk did not create JSON log: {json_path}") from exc
    except json.JSONDecodeError as exc:
        raise BinwalkError(f"Binwalk JSON log is invalid: {json_path}") from exc


def _parse_result(
    raw_json: Any,
    completed: subprocess.CompletedProcess[str],
    command: list[str],
    *,
    fallback_file_path: str,
) -> BinwalkScanResult:
    analysis = _first_analysis(raw_json)
    findings = [_parse_finding(item) for item in analysis.get("file_map", [])]
    extractions = [
        _parse_extraction(finding_id, value)
        for finding_id, value in analysis.get("extractions", {}).items()
    ]
    return BinwalkScanResult(
        file_path=analysis.get("file_path") or fallback_file_path,
        findings=findings,
        extractions=extractions,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        command=command,
        raw_json=raw_json,
    )


def _first_analysis(raw_json: Any) -> dict[str, Any]:
    if not isinstance(raw_json, list) or not raw_json:
        raise BinwalkError("Binwalk JSON log must be a non-empty list")
    first_item = raw_json[0]
    if not isinstance(first_item, dict) or not isinstance(first_item.get("Analysis"), dict):
        raise BinwalkError("Binwalk JSON log missing Analysis object")
    return first_item["Analysis"]


def _parse_finding(item: dict[str, Any]) -> BinwalkFinding:
    return BinwalkFinding(
        offset=int(item.get("offset", 0)),
        id=item.get("id"),
        name=str(item.get("name", "")),
        description=str(item.get("description", "")),
        size=item.get("size"),
        confidence=item.get("confidence"),
        always_display=item.get("always_display"),
        extraction_declined=item.get("extraction_declined"),
    )


def _parse_extraction(finding_id: str, item: dict[str, Any]) -> BinwalkExtraction:
    return BinwalkExtraction(
        finding_id=finding_id,
        success=bool(item.get("success", False)),
        output_directory=item.get("output_directory"),
        extractor=item.get("extractor"),
        size=item.get("size"),
        do_not_recurse=item.get("do_not_recurse"),
    )


def result_to_dict(result: BinwalkScanResult) -> dict[str, Any]:
    """Convert a scan result to JSON-serializable primitives."""

    data = asdict(result)
    data["extraction_failures"] = [asdict(item) for item in result.extraction_failures]
    return data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Binwalk v3 and parse JSON output.")
    parser.add_argument("file_path", type=Path)
    parser.add_argument("--binwalk", default="binwalk", help="Binwalk executable path.")
    parser.add_argument("--extract", action="store_true", help="Enable extraction with -e.")
    parser.add_argument("--matryoshka", action="store_true", help="Enable recursive extraction with -M.")
    parser.add_argument("--output-dir", type=Path, help="Extraction output directory passed to -C.")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--show-stdout", action="store_true", help="Do not pass -q to Binwalk.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = scan_file(
            args.file_path,
            binwalk=args.binwalk,
            extract=args.extract,
            matryoshka=args.matryoshka,
            output_dir=args.output_dir,
            timeout=args.timeout,
            quiet=not args.show_stdout,
        )
    except (BinwalkError, FileNotFoundError) as exc:
        parser.exit(1, f"error: {exc}\n")
    print(json.dumps(result_to_dict(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
