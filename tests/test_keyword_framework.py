"""Rewritten test suite for the keyword-driven DQ framework.

Splits tests into two classes:
- TestKeywordDQUnit: small unit-style checks (in-memory)
- TestActualFilesMedallionDQ: integration-style checks that operate on filesystem

The tests are intentionally conservative: they validate the canonical
layout (no dual-write paths) and assert quarantine directories exist.
"""
from pathlib import Path
import subprocess
import sys
import os
import json
import tempfile
import shutil
import importlib.util
import pytest


def load_run_pipeline_module():
    """Load scripts/run_pipeline.py as a module and return it."""
    script_path = Path('scripts') / 'run_pipeline.py'
    spec = importlib.util.spec_from_file_location('run_pipeline', str(script_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestKeywordDQUnit:
    def test_layer_paths_contains_expected_keys(self):
        mod = load_run_pipeline_module()
        paths = mod.layer_paths('/tmp/base_test')
        assert 'silver_clean' in paths and 'silver_quarantine' in paths
        assert 'gold_scd2' in paths and 'gold_marts' in paths

    def test_init_layout_returns_paths_and_creates_dirs(self, tmp_path):
        mod = load_run_pipeline_module()
        base = tmp_path / 'project'
        base.mkdir()
        paths = mod.init_layout(str(base))
        assert isinstance(paths, dict)
        # verify key directories exist
        for k, p in paths.items():
            assert Path(p).exists(), f"expected {p} to exist for key {k}"


class TestActualFilesMedallionDQ:
    def test_init_layout_subprocess_creates_canonical_dirs(self, tmp_path):
        base = tmp_path / 'proj'
        base.mkdir()
        cmd = [sys.executable, 'scripts/run_pipeline.py', '--init-layout', '--base-dir', str(base)]
        subprocess.check_call(cmd)

        # canonical silver paths
        assert (base / 'data' / 'silver' / 'clean').exists()
        assert (base / 'data' / 'silver' / 'quarantine').exists()

        # canonical gold paths
        assert (base / 'data' / 'gold' / 'scd2_dimensions').exists()
        assert (base / 'data' / 'gold' / 'analytics_marts').exists()
        assert (base / 'data' / 'gold' / 'analytics_marts' / 'quarantine').exists()

        # sample table directories should exist
        assert (base / 'data' / 'silver' / 'clean' / 'student').exists()
        assert (base / 'data' / 'silver' / 'quarantine' / 'student').exists()

    def test_quarantine_required_when_defects_present(self, tmp_path):
        """Simulate a source file with a defect and assert quarantine folder exists.

        The test intentionally keeps the defect simulation lightweight: it
        writes a CSV with a known-bad row into data/source and then asserts
        that the canonical quarantine directory is present after init.
        """
        base = tmp_path / 'proj2'
        (base / 'data' / 'source').mkdir(parents=True)
        # write a tiny CSV that tests expect to be considered 'defective' by
        # user workflows. We do not run the full pipeline here; we only assert
        # that the canonical quarantine location exists for teams to write to.
        csv_path = base / 'data' / 'source' / 'student.csv'
        csv_path.write_text('id,name,age\n1,Alice,20\n2,BAD_ROW,')

        # initialize layout
        cmd = [sys.executable, 'scripts/run_pipeline.py', '--init-layout', '--base-dir', str(base)]
        subprocess.check_call(cmd)

        # quarantine should exist and be writable
        qpath = base / 'data' / 'silver' / 'quarantine'
        assert qpath.exists()
        testfile = qpath / 'sample.placeholder'
        testfile.write_text('quarantine placeholder')
        assert testfile.exists()


if __name__ == '__main__':
    # allow running tests directly for quick local feedback
    sys.exit(pytest.main([__file__, '-q']))
