#!/usr/bin/env python3
"""
fetch_atlases.py

Download Atlas ensemble files (.jsonl.gz) from URLs, save them into a standardized
folder structure under data/raw/atlas/...

Why this exists:
- The atlas files are large and should be gitignored.
- This script makes the repo reproducible: anyone can fetch the same runs
  into the same paths, then run notebooks/scripts without manual file wrangling.

Inputs:
- A YAML config file (default: configs/runs.yaml) with a list of runs.
  Each run specifies:
    - name: stable identifier
    - url: raw download URL (must return bytes, not an HTML "blob" page)
    - dest: destination path in your repo (e.g. data/raw/atlas/nc_ush/.../run1.jsonl.gz)
    - (optional) sha256: if provided, verify checksum after download

Outputs:
- Downloaded files at their configured dest paths.
- Optional manifest JSON at data/raw/atlas/manifest.json recording:
    name, url, dest, bytes, sha256

Usage:
  python scripts/fetch_atlases.py
  python scripts/fetch_atlases.py --config configs/runs.yaml
  python scripts/fetch_atlases.py --dry-run
  python scripts/fetch_atlases.py --force
  python scripts/fetch_atlases.py --verify-only

Notes:
- Uses streaming download to avoid loading whole files into memory.
- If a file exists and --force is not set, it will skip.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml
from tqdm import tqdm


@dataclass(frozen=True)
class RunSpec:
    """One file to fetch and place into the repo (atlas run or input graph)."""

    name: str
    url: str
    dest: Path
    sha256: Optional[str] = None


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA-256 of a file on disk."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def ensure_parent_dir(path: Path) -> None:
    """Create parent directories if they don't exist."""
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_config(config_path: Path) -> List[RunSpec]:
    """Parse configs/runs.yaml into a list of RunSpec (from both `inputs` and `runs`)."""
    with config_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict):
        raise ValueError(f"Invalid config format in {config_path}. Expected a mapping.")

    items: List[dict] = []
    for key in ("inputs", "runs"):
        if key in cfg:
            if not isinstance(cfg[key], list):
                raise ValueError(f"'{key}' must be a list in {config_path}.")
            items.extend(cfg[key])

    if not items:
        raise ValueError(f"No 'inputs' or 'runs' found in {config_path}.")

    specs: List[RunSpec] = []
    seen_names: set[str] = set()

    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"Entry #{i} must be a mapping/dict.")
        for k in ("name", "url", "dest"):
            if k not in item:
                raise ValueError(f"Entry #{i} missing required key: {k}")

        name = str(item["name"])
        if name in seen_names:
            raise ValueError(
                f"Duplicate name '{name}' in {config_path}. Names must be unique."
            )
        seen_names.add(name)

        specs.append(
            RunSpec(
                name=name,
                url=str(item["url"]),
                dest=Path(str(item["dest"])),
                sha256=str(item["sha256"]) if item.get("sha256") else None,
            )
        )

    return specs


def download_stream(
    url: str,
    dest: Path,
    *,
    force: bool,
    timeout: int = 60,
    chunk_size: int = 1024 * 1024,
) -> Dict[str, Any]:
    """
    Download a file via streaming HTTP GET and save to dest.

    Returns metadata dict containing:
      - bytes: int
      - sha256: str
    """
    ensure_parent_dir(dest)

    if dest.exists() and not force:
        # Auto-repair a previously bad download (e.g., an HTML page saved as .jsonl.gz).
        if not is_probably_html(dest):
            return {"bytes": dest.stat().st_size, "sha256": sha256_file(dest)}
        print(f"[WARN] Existing file looks like HTML, re-downloading: {dest}")

    tmp = dest.with_suffix(dest.suffix + ".part")

    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)

        h = hashlib.sha256()
        written = 0

        with tmp.open("wb") as f, tqdm(
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
            desc=dest.name,
            leave=False,
        ) as pbar:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                written += len(chunk)
                h.update(chunk)
                if total > 0:
                    pbar.update(len(chunk))

    tmp.replace(dest)
    return {"bytes": written, "sha256": h.hexdigest()}


def is_probably_html(path: Path, sniff_bytes: int = 2048) -> bool:
    """Heuristic check: did we accidentally download an HTML page instead of the intended file?"""
    try:
        with path.open("rb") as f:
            head = f.read(sniff_bytes)
        text = head.decode("utf-8", errors="ignore").lower()
        return "<html" in text or "<!doctype html" in text
    except Exception:

        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch and place ensemble atlas files and input data files."
    )
    parser.add_argument(
        "--config", default="configs/runs.yaml", help="Path to runs YAML config."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print actions without downloading."
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-download even if dest exists."
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing files; no downloads.",
    )
    parser.add_argument(
        "--manifest",
        default="data/raw/atlas/manifest.json",
        help="Path to write a download manifest JSON.",
    )
    args = parser.parse_args()

    specs = parse_config(Path(args.config))

    manifest_path = Path(args.manifest)
    ensure_parent_dir(manifest_path)

    manifest: List[Dict[str, Any]] = []

    for spec in specs:
        if args.dry_run:
            print(f"[DRY] {spec.name}: {spec.url} -> {spec.dest}")
            continue

        if args.verify_only:
            if not spec.dest.exists():
                raise FileNotFoundError(f"Missing: {spec.dest} (name={spec.name})")
            computed = sha256_file(spec.dest)
            if spec.sha256 and computed.lower() != spec.sha256.lower():
                raise ValueError(
                    f"SHA mismatch for {spec.name}: expected {spec.sha256}, got {computed}"
                )
            if is_probably_html(spec.dest):
                raise ValueError(
                    f"{spec.dest} (name={spec.name}) looks like HTML. "
                    f"Configured URL: {spec.url}. Use a RAW URL, then re-fetch."
                )
            meta = {"bytes": spec.dest.stat().st_size, "sha256": computed}
        else:
            meta = download_stream(spec.url, spec.dest, force=args.force)

            if is_probably_html(spec.dest):
                raise ValueError(
                    f"{spec.dest} (name={spec.name}) looks like HTML after download. "
                    f"Configured URL: {spec.url}. Use a RAW file URL."
                )

            if spec.sha256 and meta["sha256"].lower() != spec.sha256.lower():
                raise ValueError(
                    f"SHA mismatch for {spec.name}: expected {spec.sha256}, got {meta['sha256']}"
                )

        manifest.append(
            {
                "name": spec.name,
                "url": spec.url,
                "dest": str(spec.dest),
                "bytes": int(meta["bytes"]),
                "sha256": meta["sha256"],
            }
        )

    if not args.dry_run:
        with manifest_path.open("w", encoding="utf-8") as f:
            json.dump({"runs": manifest}, f, indent=2)
        print(f"Wrote manifest: {manifest_path} ({len(manifest)} files)")


if __name__ == "__main__":
    main()
