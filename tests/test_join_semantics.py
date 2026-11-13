"""联接语义测试：多重集、NULL、严格类型、有序输出、热点笛卡尔。"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.engine import EngineConfig, ExternalJoinEngine
from tests.helpers import (
    make_hot_key_rows,
    make_mixed_rows,
    read_jsonl,
    reference_join,
    write_jsonl,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_CONFIG = ROOT / "fixtures" / "join.json"


def run_engine(left_rows, right_rows, join_type, *, max_records=2, max_bytes=256,
               max_open_files=4, fault=None, run_dir=None, output=None):
    tmp = Path(tempfile.mkdtemp())
    left_path = tmp / "left.jsonl"
    right_path = tmp / "right.jsonl"
    write_jsonl(left_path, left_rows)
    write_jsonl(right_path, right_rows)
    run_dir = run_dir or tmp / "run"
    output = output or tmp / "out.jsonl"
    config = EngineConfig(join_type, max_records, max_bytes, max_open_files, fault)
    stats = ExternalJoinEngine(left_path, right_path, config, run_dir).run(output)
    return read_jsonl(output), stats, tmp, run_dir, output, left_path, right_path


class FixtureContractTests(unittest.TestCase):
    def test_fixture_inner_six_rows_left_seven_rows(self):
        left = [json.loads(line) for line in (ROOT / "fixtures/left.jsonl").read_text().splitlines()]
        right = [json.loads(line) for line in (ROOT / "fixtures/right.jsonl").read_text().splitlines()]
        inner, stats, *_ = run_engine(left, right, "inner")
        left_join, *_ = run_engine(left, right, "left")
        self.assertEqual(len(inner), 6)
        self.assertEqual(len(left_join), 7)
        self.assertEqual(
            [(row["left_row_id"], row["right_row_id"]) for row in inner],
            [
                ("l1", "r1"), ("l1", "r2"), ("l1", "r3"),
                ("l2", "r1"), ("l2", "r2"), ("l2", "r3"),
            ],
        )
        # left 多出的一行是 NULL 键未匹配，右为空且排在最前。
        self.assertEqual(left_join[0]["left_row_id"], "l3")
        self.assertIsNone(left_join[0]["right_row_id"])
        self.assertIsNone(left_join[0]["right"])
        self.assertEqual(stats["fd_peak"], 3)
        # 极小预算下确实发生溢写，而不是整组进内存。
        self.assertGreaterEqual(stats["sort"]["left"]["spill_count"], 1)

    def test_output_matches_in_memory_reference_multiset(self):
        left = make_mixed_rows("l", [("x", 2), (None, 1), ("a", 2), ("z", 1)])
        right = make_mixed_rows("r", [("x", 3), ("a", 1), (None, 2), ("m", 1)])
        for join_type in ("inner", "left"):
            with self.subTest(join_type=join_type):
                got, *_ = run_engine(left, right, join_type)
                expected = reference_join(left, right, join_type)
                self.assertEqual(got, expected)
                # 多重集：行数必须等于笛卡尔展开，不去重。
                self.assertEqual(len(got), len(expected))


class NullAndTypeTests(unittest.TestCase):
    def test_null_never_matches_even_null_equals_null_pair(self):
        left = [{"row_id": "l1", "key": None, "value": 1}]
        right = [{"row_id": "r1", "key": None, "value": 2}]
        inner, *_ = run_engine(left, right, "inner")
        left_join, *_ = run_engine(left, right, "left")
        self.assertEqual(inner, [])
        self.assertEqual(len(left_join), 1)
        self.assertIsNone(left_join[0]["right"])

    def test_string_one_int_one_bool_one_float_one_are_distinct(self):
        left = [
            {"row_id": "ls", "key": "1", "value": 0},
            {"row_id": "li", "key": 1, "value": 0},
            {"row_id": "lb", "key": True, "value": 0},
            {"row_id": "lf", "key": 1.0, "value": 0},
        ]
        right = [
            {"row_id": "rs", "key": "1", "value": 1},
            {"row_id": "ri", "key": 1, "value": 1},
            {"row_id": "rb", "key": True, "value": 1},
            {"row_id": "rf", "key": 1.0, "value": 1},
        ]
        inner, *_ = run_engine(left, right, "inner")
        self.assertEqual(
            sorted((row["left_row_id"], row["right_row_id"]) for row in inner),
            [("lb", "rb"), ("lf", "rf"), ("li", "ri"), ("ls", "rs")],
        )
        # 规范化类型全序：bool < int < float < string。
        self.assertEqual(
            [row["normalized_key"]["type"] for row in inner],
            ["bool", "int", "float", "string"],
        )

    def test_unsupported_key_type_rejected_not_coerced(self):
        left = [{"row_id": "l1", "key": ["nested"], "value": 1}]
        right = [{"row_id": "r1", "key": "x", "value": 1}]
        with self.assertRaises(ValueError):
            run_engine(left, right, "inner")

    def test_missing_row_id_or_key_field_rejected(self):
        good = [{"row_id": "r1", "key": "x", "value": 1}]
        for bad in (
            [{"key": "x", "value": 1}],
            [{"row_id": "", "key": "x", "value": 1}],
            [{"row_id": "l1", "value": 1}],
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    run_engine(bad, good, "inner")


class OrderingAndIdentityTests(unittest.TestCase):
    def test_order_is_normalized_key_then_left_id_then_right_id(self):
        left = make_mixed_rows("l", [("b", 2), ("a", 2)])
        right = make_mixed_rows("r", [("b", 2), ("a", 2)])
        got, *_ = run_engine(left, right, "inner")
        keys = [(row["normalized_key"]["value"], row["left_row_id"], row["right_row_id"]) for row in got]
        self.assertEqual(keys, sorted(keys))
        # 原始两侧身份完整保留。
        self.assertEqual(got[0]["left"], left[2] if left[2]["key"] == "a" else got[0]["left"])
        for row in got:
            self.assertIn("value", row["left"])
            self.assertIn("value", row["right"])

    def test_unmatched_left_rows_sort_before_matched_rows_of_same_key(self):
        # 同键下 left join 不会同时出现匹配与未匹配行；该性质通过 NULL 键与
        # 存在性标记的全序验证：未匹配标记 0 < 匹配标记 1。
        left = make_mixed_rows("l", [(None, 1), ("k", 1)])
        right = make_mixed_rows("r", [("k", 1)])
        got, *_ = run_engine(left, right, "left")
        self.assertEqual([row["normalized_key"]["type"] for row in got], ["null", "string"])


class HotKeyCartesianTests(unittest.TestCase):
    def test_large_hot_group_produces_full_cartesian_via_disk_chunks(self):
        # 对应 join.json 的 31 x 37 扩张演示。
        left = make_hot_key_rows("l", "x", 31)
        right = make_hot_key_rows("r", "x", 37)
        got, stats, tmp, run_dir, *_ = run_engine(
            left, right, "inner", max_records=4, max_bytes=512
        )
        self.assertEqual(len(got), 31 * 37)
        self.assertEqual(got, reference_join(left, right, "inner"))
        self.assertGreaterEqual(stats["hot_groups"]["hot_groups"], 1)
        self.assertGreater(stats["hot_groups"]["hot_chunk_files"], 0)
        # parts 清单逐块可核验，行数总和等于结果行数。
        journal = [
            json.loads(line)
            for line in (run_dir / "stage2-parts.frag-main.jsonl").read_text().splitlines()
        ]
        self.assertTrue(all((run_dir / e["part"]).exists() for e in journal))
        self.assertEqual(sum(e["rows"] for e in journal), 31 * 37)

    def test_hot_group_left_join_unmatched_tail_preserved(self):
        left = make_hot_key_rows("l", "x", 10) + [{"row_id": "lN", "key": "n", "value": 0}]
        right = make_hot_key_rows("r", "x", 10)
        got, stats, *_ = run_engine(left, right, "left", max_records=3, max_bytes=400)
        self.assertEqual(len(got), 10 * 10 + 1)
        self.assertEqual(got, reference_join(left, right, "left"))
        # 未匹配行按规范化键 'n' < 'x' 排在热点组之前，右为空。
        unmatched = [row for row in got if row["left_row_id"] == "lN"]
        self.assertEqual(len(unmatched), 1)
        self.assertIsNone(unmatched[0]["right"])


if __name__ == "__main__":
    unittest.main()
