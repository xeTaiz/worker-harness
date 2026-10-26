#!/usr/bin/env python3
"""Package the two root-owned launch scripts for local Slurm SSH staging."""

import argparse
import json
import os
from pathlib import Path
import tempfile


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "extensions/pi-worker-harness/slurm/runtime-assets.json",
    )
    output = parser.parse_args().output
    assets = {
        "start-wh.sh": (root / "start-wh.sh").read_text(encoding="utf-8"),
        "wh-allocation.sh": (root / "slurm/wh-allocation.sh").read_text(encoding="utf-8"),
    }
    # No timestamps, host paths, credentials, SIF, or third-party dependencies.
    content = json.dumps({"version": 1, "assets": assets}, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o644)
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
