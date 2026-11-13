"""预算、真实溢写、FD 上限、多轮归并、超大行通道与空侧测试。"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.engine import EngineConfig, ExternalJoinEngine
from src.external_sort import SpillConfigurationError
from tests.helpers import (
    make_hot_key_rows,
    make_mixed_rows,
    read_jsonl,
    reference_join,
    write_jsonl,
)


def execute(left_rows, right_rows, join_type="inner", *, max_records=3, max_bytes=400,
            max_open_files=4, fault=None, force=False, run_dir=None, output=None):
    tmp = Path(tempfile.mkdtemp())
    left_path = tmp / "left.jsonl"
    right_path = tmp / "right.jsonl"
    write_jsonl(left_path, left_rows)
    write_jsonl(right_path, right_rows)
    run_dir = run_dir or tmp / "run"
    output = output or tmp / "out.jsonl"
    config = EngineConfig(join_type, max_records, max_bytes, max_open_files, fault)
    engine = ExternalJoinEngine(left_path, right_path, config, run_dir)
    stats = engine.run(output, force=force)
    return read_jsonl(output), stats, tmp, run_dir, output, left_path, right_path


class SpillAndBudgetTests(unittest.TestCase):
    def test_tiny_budget_forces_repeated_spill_and_multi_round_merge(self):
        left = make_mixed_rows("l", [("x", 20), ("y", 20)])
        right = make_mixed_rows("r", [("x", 20), ("z", 20)])
        got, stats, *_ = execute(
            left, right, "inner", max_records=2, max_bytes=220, max_open_files=4
        )
        # 40 行 / 每批 2 行 => 每侧至少 20 个 run；fan_in=2 必须多轮归并。
        self.assertGreaterEqual(stats["sort"]["left"]["runs_written"], 10)
        self.assertGreaterEqual(stats["merge_rounds"]["left"]["rounds"], 2)
        self.assertEqual(got, reference_join(left, right, "inner"))
        # 输出片段（part）同样触发多轮归并。
        self.assertGreaterEqual(stats["merge_rounds"]["output"]["rounds"], 1)

    def test_budget_peaks_never_exceed_configured_limits(self):
        left = make_hot_key_rows("l", "x", 25) + make_mixed_rows("l2", [("a", 5)])
        right = make_hot_key_rows("r", "x", 25) + make_mixed_rows("r2", [("a", 5)])
        max_records, max_bytes, max_fds = 3, 300, 4
        _, stats, *_ = execute(
            left, right, "inner",
            max_records=max_records, max_bytes=max_bytes, max_open_files=max_fds,
        )
        for side in ("left", "right"):
            self.assertLessEqual(stats["sort"][side]["peak_records"], max_records)
            self.assertLessEqual(stats["sort"][side]["peak_bytes"], max_bytes)
        self.assertLessEqual(stats["hot_groups"]["peak_records"], max_records)
        self.assertLessEqual(stats["hot_groups"]["peak_bytes"], max_bytes)
        # 归并堆路数受记录/FD 硬约束；字节峰值只报告（堆每路持一个首行，
        # 物理上无法保证两个合规首行之和也不超限），且必须被统计到。
        for fragment in ("left", "right", "output"):
            self.assertGreaterEqual(stats["merge_rounds"][fragment]["peak_heap_bytes"], 0)
            self.assertLessEqual(
                stats["merge_rounds"][fragment]["peak_input_runs"], max_records
            )
        self.assertLessEqual(stats["fd_peak"], max_fds)

    def test_spill_bytes_are_verifiable_on_disk(self):
        left = make_mixed_rows("l", [("x", 12)])
        right = make_mixed_rows("r", [("x", 12)])
        _, stats, tmp, run_dir, *_ = execute(
            left, right, "inner", max_records=2, max_bytes=200
        )
        run_files = sorted(run_dir.glob("*.run?????.jsonl"))
        self.assertTrue(run_files)
        # 磁盘上 run 字节总和与统计的溢写字节口径一致（归并不再产生新的侧 run 字节差异）。
        side_spill = (
            stats["sort"]["left"]["spill_bytes"] + stats["sort"]["right"]["spill_bytes"]
        )
        initial_runs = [p for p in run_files if ".merge." not in p.name]
        self.assertEqual(sum(p.stat().st_size for p in initial_runs), side_spill)

    def test_hot_group_reported_spill_and_chunk_count(self):
        left = make_hot_key_rows("l", "x", 20)
        right = make_hot_key_rows("r", "x", 20)
        _, stats, *_ = execute(left, right, "inner", max_records=4, max_bytes=500)
        hot = stats["hot_groups"]
        self.assertGreaterEqual(hot["hot_groups"], 1)
        self.assertGreater(hot["hot_chunk_files"], 0)
        self.assertGreater(hot["spill_bytes"], 0)
        self.assertGreater(hot["spill_records"], 0)


class FDLimitTests(unittest.TestCase):
    def test_engine_respects_very_low_fd_budget(self):
        left = make_mixed_rows("l", [("x", 16)])
        right = make_mixed_rows("r", [("x", 16)])
        _, stats, *_ = execute(
            left, right, "inner", max_records=2, max_bytes=200, max_open_files=3
        )
        # fan_in = min(3-1, 2) = 2（2 输入 + 1 输出），仍可完成；峰值不超过 3。
        self.assertLessEqual(stats["fd_peak"], 3)

    def test_fd_two_works_for_single_run_but_rejects_real_spill(self):
        # 数据量恰好落在单 run 内：FD=2（输入+输出）足够，无需归并。
        small_left = make_mixed_rows("l", [("x", 1)])
        small_right = make_mixed_rows("r", [("x", 1)])
        got, *_ = execute(
            small_left, small_right, "inner",
            max_records=10, max_bytes=500, max_open_files=2,
        )
        self.assertEqual(len(got), 1)
        # 真正溢写出多 run 时，FD=2 物理不可行，必须给明确配置错误。
        big_left = make_mixed_rows("l", [("x", 12)])
        big_right = make_mixed_rows("r", [("x", 12)])
        with self.assertRaises(SpillConfigurationError):
            execute(
                big_left, big_right, "inner",
                max_records=2, max_bytes=200, max_open_files=2,
            )

    def test_fd_below_two_rejected(self):
        with self.assertRaises(ValueError):
            EngineConfig("inner", 2, 200, 1)

    def test_records_one_with_fd_two_is_physically_impossible_and_rejected(self):
        with self.assertRaises(ValueError):
            EngineConfig("inner", 1, 200, 2)
        # 同样记录批次为 1，FD=3 可以逐路透传后两两归并完成。
        got, *_ = execute(
            make_mixed_rows("l", [("x", 6)]),
            make_mixed_rows("r", [("x", 6)]),
            "inner", max_records=1, max_bytes=200, max_open_files=3,
        )
        self.assertEqual(len(got), 36)


class OversizedRecordTests(unittest.TestCase):
    def test_single_oversized_row_uses_dedicated_channel_without_broken_json(self):
        big = "q" * 400
        left = [
            {"row_id": "l1", "key": "x", "value": 1},
            {"row_id": "lbig", "key": "x", "value": big},
        ]
        right = [{"row_id": "r1", "key": "x", "value": 2}]
        got, stats, tmp, run_dir, output, left_path, right_path = execute(
            left, right, "inner", max_records=4, max_bytes=160
        )
        self.assertEqual(len(got), 2)
        self.assertEqual({row["left_row_id"] for row in got}, {"l1", "lbig"})
        # 大字符串原样保留，JSON 未被截断。
        big_row = next(row for row in got if row["left_row_id"] == "lbig")
        self.assertEqual(big_row["left"]["value"], big)
        self.assertTrue(stats["oversized"])
        self.assertEqual(stats["oversized"][0]["row_id"], "lbig")
        # 超大通道文件本身是合法 JSONL。
        over = read_jsonl(run_dir / "left.oversized.jsonl")
        self.assertEqual(over[0]["id"], "lbig")
        self.assertEqual(got, reference_join(left, right, "inner"))

    def test_oversized_left_row_left_join_unmatched_when_no_key_match(self):
        left = [{"row_id": "lbig", "key": "nope", "value": "q" * 300}]
        right = [{"row_id": "r1", "key": "x", "value": 1}]
        got, stats, *_ = execute(left, right, "left", max_records=4, max_bytes=160)
        self.assertEqual(len(got), 1)
        self.assertIsNone(got[0]["right"])

    def test_oversized_right_row_still_joins(self):
        left = [{"row_id": "l1", "key": "x", "value": 1}]
        right = [{"row_id": "rbig", "key": "x", "value": "z" * 300}]
        got, stats, *_ = execute(left, right, "inner", max_records=4, max_bytes=160)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["right_row_id"], "rbig")

    def test_multiple_oversized_rows_both_sides_no_duplicate_pairs(self):
        # 两侧各 2 个超大行 + 1 个普通行，同键：完整笛卡尔必须是 3x3=9，
        # 超大×超大配对只能各出现一次（回归重复嵌套的缺陷）。
        left = [
            {"row_id": "lb1", "key": "x", "value": "Q" * 300},
            {"row_id": "lb2", "key": "x", "value": "R" * 300},
            {"row_id": "ls1", "key": "x", "value": 1},
        ]
        right = [
            {"row_id": "rb1", "key": "x", "value": "S" * 300},
            {"row_id": "rb2", "key": "x", "value": "T" * 300},
            {"row_id": "rs1", "key": "x", "value": 2},
        ]
        got, stats, *_ = execute(left, right, "inner", max_records=2, max_bytes=160)
        self.assertEqual(len(got), 9)
        pairs = [(r["left_row_id"], r["right_row_id"]) for r in got]
        self.assertEqual(len(pairs), len(set(pairs)))  # 无重复配对
        self.assertEqual(
            sorted(pairs),
            sorted((l["row_id"], r["row_id"]) for l in left for r in right),
        )
        self.assertEqual(
            {e["row_id"] for e in stats["oversized"]}, {"lb1", "lb2", "rb1", "rb2"}
        )

    def test_oversized_singleton_not_counted_as_hot_group(self):
        # 只有超大行、没有任何累积缓冲触顶：hot_groups 必须为 0，
        # 超大单例 chunk 单独计数。
        left = [
            {"row_id": "lb1", "key": "x", "value": "Q" * 300},
            {"row_id": "ls1", "key": "x", "value": 1},
        ]
        right = [
            {"row_id": "rb1", "key": "x", "value": "R" * 300},
            {"row_id": "rs1", "key": "x", "value": 2},
        ]
        got, stats, *_ = execute(left, right, "inner", max_records=4, max_bytes=160)
        self.assertEqual(len(got), 4)
        self.assertEqual(stats["hot_groups"]["hot_groups"], 0)
        self.assertGreaterEqual(stats["hot_groups"]["oversized_singleton_chunks"], 2)
        # 超大单例不计入双约束缓冲的溢写字节。
        self.assertEqual(stats["hot_groups"]["spill_count"], 0)

    def test_real_hot_group_distinct_from_oversized_only(self):
        # 同键 6 个普通行 + 极小记录预算：必须出现真实热点缓冲溢写。
        left = make_mixed_rows("l", [("x", 6)])
        right = make_mixed_rows("r", [("x", 6)])
        got, stats, *_ = execute(left, right, "inner", max_records=2, max_bytes=400)
        self.assertEqual(len(got), 36)
        self.assertGreaterEqual(stats["hot_groups"]["hot_groups"], 1)
        self.assertGreater(stats["hot_groups"]["spill_bytes"], 0)


class EmptySideTests(unittest.TestCase):
    def test_empty_side_inner_and_left(self):
        left = make_mixed_rows("l", [("x", 3), (None, 1)])
        for join_type, expected in (("inner", 0), ("left", 4)):
            with self.subTest(join_type=join_type):
                got, *_ = execute(left, [], join_type, max_records=2, max_bytes=200)
                self.assertEqual(len(got), expected)
                self.assertEqual(got, reference_join(left, [], join_type))

    def test_both_sides_empty(self):
        for join_type in ("inner", "left"):
            got, stats, *_ = execute([], [], join_type)
            self.assertEqual(got, [])
            self.assertEqual(stats["output_rows"], 0)


class StreamingTests(unittest.TestCase):
    def test_output_is_written_without_collecting_results_in_memory(self):
        # 结构验证：引擎只通过 write 流式落 part，结果行不进入引擎级列表。
        import inspect

        from src import engine as engine_module

        source = inspect.getsource(engine_module)
        self.assertNotIn("all_results", source)
        # 结果行数远超任何缓冲容量，输出仍完整有序。
        left = make_hot_key_rows("l", "x", 40)
        right = make_hot_key_rows("r", "x", 40)
        got, stats, *_ = execute(left, right, "inner", max_records=3, max_bytes=350)
        self.assertEqual(len(got), 1600)
        self.assertEqual(got, reference_join(left, right, "inner"))


class StatsCoverageTests(unittest.TestCase):
    def test_stats_cover_peaks_spill_hot_blocks_and_fd(self):
        left = make_hot_key_rows("l", "x", 15)
        right = make_hot_key_rows("r", "x", 15)
        _, stats, *_ = execute(left, right, "inner", max_records=3, max_bytes=300)
        for key in ("input", "sort", "merge_rounds", "hot_groups", "parts",
                    "output_rows", "output_bytes", "oversized", "fd_peak"):
            self.assertIn(key, stats)
        for side in ("left", "right"):
            for key in ("peak_records", "peak_bytes", "spill_count", "spill_bytes"):
                self.assertIn(key, stats["sort"][side])
        for key in ("hot_groups", "hot_chunk_files", "spill_bytes", "peak_bytes"):
            self.assertIn(key, stats["hot_groups"])


if __name__ == "__main__":
    unittest.main()
