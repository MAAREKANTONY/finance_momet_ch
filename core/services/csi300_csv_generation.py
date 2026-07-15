from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from django.conf import settings
from django.utils import timezone

from tools import convert_unliftedq_csi300_to_stockalert_csv as converter
from tools.csi300_policy import CSI300_SUPPORTED_HISTORY_START_ISO


class CSI300CSVGenerationError(RuntimeError):
    pass


@dataclass
class CSI300CSVGenerationResult:
    status: str
    csv_path: str = ""
    report_path: str = ""
    manifest_path: str = ""
    csv_sha256: str = ""
    csv_size: int = 0
    source_repository: str = converter.SOURCE_REPOSITORY
    source_tag: str = converter.SOURCE_TAG
    source_commit: str = converter.SOURCE_COMMIT
    supported_history_start: str = CSI300_SUPPORTED_HISTORY_START_ISO
    memberships: int = 0
    distinct_tickers: int = 0
    coverage_start: str = ""
    coverage_end: str = ""
    excluded_intervals: int = 0
    clipped_intervals: int = 0
    duplicate_rows: int = 0
    overlapping_periods: int = 0
    conflicts: int = 0
    warnings: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    active_counts: dict[str, int] = field(default_factory=dict)
    control_date_checks: dict[str, dict[str, Any]] = field(default_factory=dict)
    checksums_expected: dict[str, str] = field(default_factory=dict)
    checksums_received: dict[str, str] = field(default_factory=dict)
    generated_at: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def csi300_generation_root() -> Path:
    return Path(getattr(settings, "CSI300_GENERATION_ROOT", "/data/exports/csi300/history"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_converter_report(report: dict[str, Any], csv_path: Path) -> list[str]:
    issues: list[str] = []
    expected_identity = {
        "source_repository": converter.SOURCE_REPOSITORY,
        "source_tag": converter.SOURCE_TAG,
        "source_commit": converter.SOURCE_COMMIT,
        "supported_history_start": CSI300_SUPPORTED_HISTORY_START_ISO,
    }
    for key, expected in expected_identity.items():
        if report.get(key) != expected:
            issues.append(f"{key} inattendu: {report.get(key)!r}")
    if report.get("status") != "valid":
        issues.append(f"statut de conversion non valide: {report.get('status')!r}")
    if report.get("errors"):
        issues.append("le rapport de conversion contient des erreurs")
    if report.get("checksums_expected") != converter.EXPECTED_SOURCE_SHA256:
        issues.append("les checksums attendus ne correspondent pas à la source pinnée")
    if report.get("checksums_received") != converter.EXPECTED_SOURCE_SHA256:
        issues.append("les checksums reçus ne correspondent pas à la source pinnée")
    if report.get("duplicate_exact_rows"):
        issues.append("le CSV contient des clés dupliquées")
    if report.get("overlapping_periods"):
        issues.append("le CSV contient des périodes qui se chevauchent")
    if not csv_path.is_file() or csv_path.stat().st_size <= 0:
        issues.append("le CSV généré est absent ou vide")
    if int(report.get("memberships_written") or 0) != int(report.get("memberships_produced") or 0):
        issues.append("le nombre de memberships écrits diffère du nombre produit")
    return issues


def _result_from_report(
    report: dict[str, Any],
    *,
    status: str,
    csv_path: Path | None = None,
    report_path: Path | None = None,
    manifest_path: Path | None = None,
) -> CSI300CSVGenerationResult:
    return CSI300CSVGenerationResult(
        status=status,
        csv_path=str(csv_path or ""),
        report_path=str(report_path or ""),
        manifest_path=str(manifest_path or ""),
        csv_sha256=_sha256(csv_path) if csv_path and csv_path.is_file() else "",
        csv_size=csv_path.stat().st_size if csv_path and csv_path.is_file() else 0,
        memberships=int(report.get("memberships_written") or report.get("memberships_produced") or 0),
        distinct_tickers=int(report.get("distinct_tickers") or 0),
        coverage_start=str(report.get("min_start_date") or ""),
        coverage_end=str(report.get("max_end_date") or ""),
        excluded_intervals=int(report.get("outside_supported_history_count") or 0),
        clipped_intervals=int(report.get("clipped_to_supported_start_count") or 0),
        duplicate_rows=len(report.get("duplicate_exact_rows") or []),
        overlapping_periods=len(report.get("overlapping_periods") or []),
        warnings=list(report.get("warnings") or []),
        errors=list(report.get("errors") or []),
        active_counts=dict(report.get("active_counts") or {}),
        control_date_checks=dict(report.get("control_date_checks") or {}),
        checksums_expected=dict(report.get("checksums_expected") or {}),
        checksums_received=dict(report.get("checksums_received") or {}),
        generated_at=timezone.now().isoformat(),
    )


def generate_csi300_historical_csv(
    *,
    job_id: int,
    converter_runner: Callable[[list[str]], int] | None = None,
) -> CSI300CSVGenerationResult:
    root = csi300_generation_root()
    root.mkdir(parents=True, exist_ok=True)
    stamp = timezone.now().strftime("%Y%m%d_%H%M%S")
    staging = root / f".generation_{stamp}_job_{job_id}.tmp"
    final = root / f"generation_{stamp}_job_{job_id}"
    failure = root / f"failed_{stamp}_job_{job_id}"
    staging.mkdir(parents=False, exist_ok=False)
    csv_path = staging / "csi300_stockalert_memberships.csv"
    report_path = staging / "conversion_report.json"
    manifest_path = staging / "generation_manifest.json"
    runner = converter_runner or converter.main
    args = [
        "--download",
        "--source-version",
        converter.SOURCE_COMMIT,
        "--output",
        str(csv_path),
        "--report",
        str(report_path),
        "--as-of",
        CSI300_SUPPORTED_HISTORY_START_ISO,
        "--as-of",
        "2026-06-11",
        "--as-of",
        "2026-06-12",
        "--as-of",
        "2026-06-13",
        "--strict",
    ]
    try:
        try:
            exit_code = int(runner(args))
        except SystemExit as exc:
            exit_code = int(exc.code or 0)
        report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.is_file() else {}
        issues = _validate_converter_report(report, csv_path)
        if exit_code != 0:
            issues.insert(0, f"le convertisseur a retourné le code {exit_code}")
        if issues:
            if not report.get("errors"):
                report["errors"] = [{"code": "generation_validation", "message": issue} for issue in issues]
            _atomic_json(staging / "job_failure.json", {"status": "failed", "issues": issues, "report": report})
            os.replace(staging, failure)
            raise CSI300CSVGenerationError("; ".join(issues))

        result = _result_from_report(
            report,
            status="DONE_WITH_WARNING" if report.get("warnings") else "DONE",
            csv_path=csv_path,
            report_path=report_path,
            manifest_path=manifest_path,
        )
        _atomic_json(manifest_path, result.as_dict())
        os.replace(staging, final)
        final_csv = final / csv_path.name
        final_report = final / report_path.name
        final_manifest = final / manifest_path.name
        result.csv_path = str(final_csv)
        result.report_path = str(final_report)
        result.manifest_path = str(final_manifest)
        _atomic_json(final_manifest, result.as_dict())
        _atomic_json(root / "latest_valid.json", result.as_dict())
        return result
    except CSI300CSVGenerationError:
        raise
    except Exception as exc:
        if staging.exists():
            _atomic_json(staging / "job_failure.json", {"status": "failed", "error": str(exc)})
            os.replace(staging, failure)
        raise CSI300CSVGenerationError(str(exc)) from exc


def latest_valid_csi300_generation() -> dict[str, Any] | None:
    pointer = csi300_generation_root() / "latest_valid.json"
    if not pointer.is_file():
        return None
    try:
        payload = json.loads(pointer.read_text(encoding="utf-8"))
        csv_path = Path(str(payload.get("csv_path") or "")).resolve()
        report_path = Path(str(payload.get("report_path") or "")).resolve()
        root = csi300_generation_root().resolve()
        if root not in csv_path.parents or root not in report_path.parents:
            return None
        if not csv_path.is_file() or not report_path.is_file():
            return None
        if payload.get("csv_sha256") != _sha256(csv_path):
            return None
        if json.loads(report_path.read_text(encoding="utf-8")).get("status") != "valid":
            return None
        return payload
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def csi300_generation_artifact(kind: str) -> tuple[Path, str] | None:
    payload = latest_valid_csi300_generation()
    if not payload or kind not in {"csv", "report"}:
        return None
    key = "csv_path" if kind == "csv" else "report_path"
    path = Path(str(payload.get(key) or "")).resolve()
    name = "csi300_stockalert_memberships.csv" if kind == "csv" else "csi300_conversion_report.json"
    return path, name
