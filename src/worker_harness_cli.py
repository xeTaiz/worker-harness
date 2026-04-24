#!/usr/bin/env python3
"""CLI entry point installed as `worker-harness`."""

import sys
import os

# Add src to path so the package can be imported when running as a script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from worker_harness.cli.app import main_entry

if __name__ == "__main__":
    main_entry()
