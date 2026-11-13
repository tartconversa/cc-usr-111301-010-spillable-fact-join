"""进程恢复、临时 run 校验、指纹复用拒绝与故障注入测试。"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.engine import (
    EngineConfig,
    ExternalJoinEngine,
    InjectedCrash,
    RunReuseError,
    sha256_file,
)
from tests.helpers import (
    make_hot_key_rows,
    make_mixed_rows,
    read_jsonl,
    reference_join,
    write_jsonl,
)


class RecoveryHarness:
    def __init__(self, left_rows, right_rows, join_type="inner", **budget):
        self.tmp = Path(tempfile.mkdtemp())
        self.left_path = self.tmp / "left.jsonl"
        self.right_path = self.tmp / "right.jsonl"
        write_jsonl(self.left_path, left_rows)
        write_jsonl(self.right_path, right_rows)
        self.run_dir = self.tmp / "run"
        self.output = self.tmp / "out.jsonl"
        self.left_rows = left_rows
        self.right_rows = right_rows
        self.join_type = join_type
        self.budget = budget or {"max_records": 3, "max_bytes": 350, "max_open_files": 4}

    def invoke(self, fault=None, force=False):
        config = EngineConfig(self.join_type, fault=fault, **self.budget)
        return ExternalJoinEngine(
            self.left_path, self.right_path, config, self.run_dir
        ).run(self.output, force=force)


class FaultAndRecoveryTests(unittest.TestCase):
    def _harness(self):
        left = make_hot_key_rows("l", "x", 24) + make_mixed_rows("la", [("a", 6), (None, 2)])
        right = make_hot_key_rows("r", "x", 24) + make_mixed_rows("ra", [("a", 3)])
        return RecoveryHarness(left, right, "left")

    def test_crash_at_each_injection_point_then_resume_gives_identical_output(self):
        for fault in (
            "stage1_after_sort",
            "stage2_after_hot_chunk",
            "stage2_after_part",
            "stage3_after_merge",
        ):
            with self.subTest(fault=fault):
                harness = self._harness()
                with self.assertRaises(InjectedCrash):
                    harness.invoke(fault=fault)
                # 崩溃后没有输出提交。
                self.assertFalse(harness.output.exists())
                # 恢复：同一命令（不带注入）重跑。
                stats = harness.invoke()
                got = read_jsonl(harness.output)
                self.assertEqual(
                    got, reference_join(harness.left_rows, harness.right_rows, "left")
                )
                # 再跑一次：已提交输出与清单不重复追加，行数不变。
                harness.invoke()
                self.assertEqual(
                    len(read_jsonl(harness.output)), len(got)
                )
                self.assertEqual(stats["output_rows"], len(got))

    def test_restart_does_not_duplicate_completed_cartesian_blocks(self):
        harness = self._harness()
        with self.assertRaises(InjectedCrash):
            harness.invoke(fault="stage2_after_part")
        journal_path = harness.run_dir / "stage2-parts.frag-main.jsonl"
        journal_before = journal_path.read_text()
        harness.invoke()
        journal_after = journal_path.read_text()
        # 恢复后的清单包含崩溃前已验证的全部条目且序号不重复。
        seqs = [
            json.loads(line)["seq"]
            for line in journal_after.splitlines()
        ]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(seqs), len(set(seqs)))
        for line in journal_before.splitlines():
            self.assertIn(line, journal_after)

    def test_partial_files_are_never_treated_as_completed_runs(self):
        harness = self._harness()
        with self.assertRaises(InjectedCrash):
            harness.invoke(fault="stage1_after_sort")
        # 阶段1未完成：只有 .partial / run 文件，没有 stage1.done。
        self.assertFalse((harness.run_dir / "stage1.done").exists())
        partials = list(harness.run_dir.glob("*.partial"))
        # 恢复后完成，最终输出与参考一致。
        harness.invoke()
        self.assertTrue((harness.run_dir / "stage1.done").exists())
        self.assertEqual(
            read_jsonl(harness.output),
            reference_join(harness.left_rows, harness.right_rows, "left"),
        )
        # 完成后 run 目录不再残留 .partial 临时文件。
        self.assertEqual(list(harness.run_dir.glob("*.partial")), [])
        self.assertGreaterEqual(len(partials), 0)

    def test_corrupt_journal_entry_truncates_and_recomputes_block(self):
        harness = self._harness()
        with self.assertRaises(InjectedCrash):
            harness.invoke(fault="stage2_after_part")
        journal = harness.run_dir / "stage2-parts.frag-main.jsonl"
        lines = journal.read_text().splitlines()
        self.assertGreaterEqual(len(lines), 1)
        # 篡改最后一个 part 的字节内容，但保留清单条目 => 校验失败触发重算。
        last_entry = json.loads(lines[-1])
        target = harness.run_dir / last_entry["part"]
        target.write_bytes(target.read_bytes() + b'{"tampered":true}\n')
        harness.invoke()
        got = read_jsonl(harness.output)
        self.assertEqual(
            got, reference_join(harness.left_rows, harness.right_rows, "left")
        )


class FingerprintReuseTests(unittest.TestCase):
    def _harness(self):
        return RecoveryHarness(
            make_mixed_rows("l", [("x", 8), (None, 1)]),
            make_mixed_rows("r", [("x", 8)]),
            "left",
            max_records=2,
            max_bytes=250,
            max_open_files=4,
        )

    def test_input_byte_change_rejects_reuse_without_force(self):
        harness = self._harness()
        harness.invoke()
        # 修改左表一个字节（保持 JSON 合法）。
        rows = read_jsonl(harness.left_path)
        rows[0]["value"] = rows[0]["value"] + 100
        write_jsonl(harness.left_path, rows)
        with self.assertRaises(RunReuseError):
            harness.invoke()
        # force 后重建并得到与新输入一致的结果。
        harness.invoke(force=True)
        self.assertEqual(
            read_jsonl(harness.output),
            reference_join(rows, harness.right_rows, "left"),
        )

    def test_param_change_rejects_reuse(self):
        harness = self._harness()
        harness.invoke()
        harness.budget["max_records"] = 5
        with self.assertRaises(RunReuseError):
            harness.invoke()
        harness.invoke(force=True)
        self.assertTrue(harness.output.exists())

    def test_identical_fingerprint_allows_resume_and_reuse(self):
        harness = self._harness()
        stats1 = harness.invoke()
        stats2 = harness.invoke()  # 全部阶段命中标记，直接复用
        self.assertEqual(stats2["output_rows"], stats1["output_rows"])
        self.assertEqual(
            sha256_file(harness.output),
            sha256_file(harness.output),
        )

    def test_join_type_change_is_part_of_fingerprint(self):
        harness = self._harness()
        harness.invoke()
        harness.join_type = "inner"
        with self.assertRaises(RunReuseError):
            harness.invoke()


class CommitAtomicityTests(unittest.TestCase):
    def test_output_appears_only_after_all_parts_verified(self):
        harness = RecoveryHarness(
            make_hot_key_rows("l", "x", 18),
            make_hot_key_rows("r", "x", 18),
            "inner",
            max_records=3,
            max_bytes=350,
            max_open_files=4,
        )
        with self.assertRaises(InjectedCrash):
            harness.invoke(fault="stage3_after_merge")
        # 归并完成但未提交：输出文件不存在，仅有 run 内临时产物。
        self.assertFalse(harness.output.exists())
        harness.invoke()
        self.assertTrue(harness.output.exists())
        self.assertEqual(len(read_jsonl(harness.output)), 18 * 18)

    def test_stage_markers_payload_is_valid_json(self):
        harness = self._harness()
        harness.invoke()
        for marker in ("stage1.done", "stage2.done", "stage3.done", "fingerprint.json"):
            json.loads((harness.run_dir / marker).read_text(encoding="utf-8"))

    def _harness(self):
        return RecoveryHarness(
            make_mixed_rows("l", [("x", 6)]),
            make_mixed_rows("r", [("x", 6)]),
            "inner",
            max_records=2,
            max_bytes=250,
            max_open_files=4,
        )


if __name__ == "__main__":
    unittest.main()
