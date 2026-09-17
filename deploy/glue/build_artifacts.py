"""Package shared ETL Python code and driver-side config/SQL for AWS Glue."""

from __future__ import annotations

import argparse
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _write_archive(path: Path, root: Path, files: list[Path]) -> None:
    """Write a stable zip containing only explicit repository files."""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for source in sorted(files):
            relative = source.relative_to(root).as_posix()
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, source.read_bytes())


def build_artifacts(project_root: Path, output_dir: Path) -> tuple[Path, Path]:
    """Return a Python-path zip and a separate archive for local driver resources."""
    project_root = project_root.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    package_root = project_root / "src"
    code = sorted((package_root / "transport_etl").rglob("*.py"))
    resources = [
        path
        for directory in (project_root / "config", project_root / "sql")
        for path in directory.rglob("*")
        if path.is_file() and path.suffix in {".yaml", ".json", ".conf", ".sql"}
    ]
    if not code or not resources:
        raise FileNotFoundError(
            "transport_etl Python sources and config/SQL resources are required"
        )
    code_zip = output_dir / "transport_etl_code.zip"
    resources_zip = output_dir / "transport_etl_resources.zip"
    _write_archive(code_zip, package_root, code)
    _write_archive(resources_zip, project_root, resources)
    return code_zip, resources_zip


def main() -> None:
    """Create deployable artifacts without needing AWS credentials."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "dist" / "glue")
    args = parser.parse_args()
    code, resources = build_artifacts(PROJECT_ROOT, args.output_dir)
    print(code)
    print(resources)


if __name__ == "__main__":
    main()
