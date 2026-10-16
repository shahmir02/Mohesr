#!/usr/bin/env python3
"""
Canonical medallion layout helper and lightweight orchestration script.
This rewritten script intentionally performs layout / path management and light
local initialization (used by tests). It avoids any dual-write logic and
exposes clear canonical locations for silver/gold layers.

Commands
- --init-layout: create canonical directory layout and placeholder .gitkeep files

This file is intentionally small and side-effect free unless --init-layout is used.
"""
from pathlib import Path
import argparse
import json
import sys


def layer_paths(base_dir: str = '.') -> dict:
    """Return canonical layer paths (Path objects) rooted at base_dir.

    Keys:
      source, bronze, silver_clean, silver_quarantine,
      gold_scd2, gold_marts, gold_mart_quarantine, reports
    """
    base = Path(base_dir).resolve()
    paths = {
        'source': base / 'data' / 'source',
        'bronze': base / 'data' / 'bronze',
        'silver_clean': base / 'data' / 'silver' / 'clean',
        'silver_quarantine': base / 'data' / 'silver' / 'quarantine',
        'gold_scd2': base / 'data' / 'gold' / 'scd2_dimensions',
        'gold_marts': base / 'data' / 'gold' / 'analytics_marts',
        'gold_mart_quarantine': base / 'data' / 'gold' / 'analytics_marts' / 'quarantine',
        'reports': base / 'reports',
    }
    return paths


def init_layout(base_dir: str = '.') -> dict:
    """Create canonical directory layout and write placeholder .gitkeep files.

    Returns the dict of paths (as strings).
    """
    paths = layer_paths(base_dir)
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
        gitkeep = p / '.gitkeep'
        if not gitkeep.exists():
            gitkeep.write_text('')

    # Create sample table directories used by tests
    for tbl in ('student', 'enrollment', 'institution', 'program'):
        (Path(base_dir) / 'data' / 'bronze' / tbl).mkdir(parents=True, exist_ok=True)
        (Path(base_dir) / 'data' / 'silver' / 'clean' / tbl).mkdir(parents=True, exist_ok=True)
        (Path(base_dir) / 'data' / 'silver' / 'quarantine' / tbl).mkdir(parents=True, exist_ok=True)

    return {k: str(v) for k, v in paths.items()}


def main(argv=None):
    parser = argparse.ArgumentParser(description='Lightweight medallion layout helper')
    parser.add_argument('--init-layout', action='store_true', help='Create canonical data/gold/silver layout')
    parser.add_argument('--base-dir', default='.', help='Base directory to create layout (default: .)')
    parser.add_argument('--print-paths', action='store_true', help='Print canonical paths as JSON')
    args = parser.parse_args(argv)

    if args.init_layout:
        paths = init_layout(args.base_dir)
        print('Initialized layout at', args.base_dir)
        if args.print_paths:
            print(json.dumps(paths))
        return 0

    if args.print_paths:
        paths = layer_paths(args.base_dir)
        print(json.dumps({k: str(v) for k, v in paths.items()}))
        return 0

    parser.print_help()
    return 2


if __name__ == '__main__':
    sys.exit(main())
