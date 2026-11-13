"""CLI 端到端测试：退出码、错误输入拒绝、CLI 预算覆盖与统计输出。"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tests.helpers import make_mixed_rows, read_jsonl, reference_join, write_jsonl

ROOT = Path(__file__).resolve().parents[1]


def run_cli(*args: str) -> subprocess.CompletedProcess:
    # -I 隔离模式 + 显式 sys.path 注入，模拟 README 的安全运行方式。
    bootstrap = (
        "import sys, runpy; sys.path.insert(0, %r); "
        "sys.argv=['external_join']+%r; "
        "from src.external_join import main; raise SystemExit(main())"
        % (str(ROOT), list(args))
    )
    return subprocess.run(
        [sys.executable, "-I", "-c", bootstrap],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )


class CliEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.left = self.tmp / "left.jsonl"
        self.right = self.tmp / "right.jsonl"
        self.run_dir = self.tmp / "run"
        self.output = self.tmp / "out.jsonl"
        self.stats = self.tmp / "stats.json"
        write_jsonl(self.left, make_mixed_rows("l", [("x", 4), (None, 1)]))
        write_jsonl(self.right, make_mixed_rows("r", [("x", 5)]))

    def _base_args(self, join_type):
        # join_type 属于执行指纹，不同联接类型必须使用独立 run 目录，
        # 否则会被复用保护拒绝（这本身也是一项被覆盖的契约）。
        return [
            "run",
            "--left", str(self.left),
            "--right", str(self.right),
            "--config", str(ROOT / "fixtures" / "join.json"),
            "--join-type", join_type,
            "--run-dir", str(self.run_dir / join_type),
            "--output", str(self.tmp / f"out-{join_type}.jsonl"),
            "--stats", str(self.tmp / f"stats-{join_type}.json"),
        ]

    def _out_path(self, join_type):
        return self.tmp / f"out-{join_type}.jsonl"

    def _stats_path(self, join_type):
        return self.tmp / f"stats-{join_type}.json"

    def test_inner_and_left_cli_match_reference(self):
        for join_type in ("inner", "left"):
            with self.subTest(join_type=join_type):
                proc = run_cli(*self._base_args(join_type))
                self.assertEqual(proc.returncode, 0, proc.stderr)
                got = read_jsonl(self._out_path(join_type))
                expected = reference_join(
                    read_jsonl(self.left), read_jsonl(self.right), join_type
                )
                self.assertEqual(got, expected)
                stats = json.loads(self._stats_path(join_type).read_text(encoding="utf-8"))
                self.assertEqual(stats["output_rows"], len(expected))
                self.assertLessEqual(stats["fd_peak"], 4)

    def test_malformed_jsonl_is_rejected_not_dropped(self):
        self.left.write_text('{"row_id":"l1","key":"x","value":1}\n{bad json}\n', encoding="utf-8")
        proc = run_cli(*self._base_args("inner"))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("合法 JSON", proc.stderr)

    def test_missing_input_returns_exit_code_2(self):
        proc = run_cli(
            "run", "--left", str(self.tmp / "nope.jsonl"),
            "--right", str(self.right),
            "--config", str(ROOT / "fixtures" / "join.json"),
            "--join-type", "inner",
            "--run-dir", str(self.run_dir / "inner"),
            "--output", str(self._out_path("inner")),
        )
        self.assertEqual(proc.returncode, 2)

    def test_budget_overrides_force_more_spills(self):
        args = self._base_args("inner") + ["--max-records", "1", "--max-bytes", "180"]
        proc = run_cli(*args)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        stats = json.loads(self._stats_path("inner").read_text(encoding="utf-8"))
        self.assertEqual(stats["config"]["max_records"], 1)
        self.assertGreaterEqual(stats["sort"]["left"]["runs_written"], 4)

    def test_fault_then_resume_across_processes(self):
        args = self._base_args("left") + ["--fault", "stage2_after_part"]
        crashed = run_cli(*args)
        self.assertEqual(crashed.returncode, 4)
        self.assertFalse(self._out_path("left").exists())
        resumed = run_cli(*[a for a in self._base_args("left")])
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        got = read_jsonl(self._out_path("left"))
        self.assertEqual(
            got,
            reference_join(read_jsonl(self.left), read_jsonl(self.right), "left"),
        )

    def test_reuse_rejection_exit_code_3(self):
        first = run_cli(*self._base_args("inner"))
        self.assertEqual(first.returncode, 0)
        rows = read_jsonl(self.left)
        rows[0]["value"] += 999
        write_jsonl(self.left, rows)
        rejected = run_cli(*self._base_args("inner"))
        self.assertEqual(rejected.returncode, 3)
        forced = run_cli(*self._base_args("inner"), "--force")
        self.assertEqual(forced.returncode, 0, forced.stderr)


if __name__ == "__main__":
    unittest.main()
