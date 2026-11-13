"""固定种子的随机差分测试：外部执行器 vs 朴素内存参考。

覆盖多类型键、NULL、重复热点、left/inner 与多组极小预算的笛卡尔组合；
种子固定保证可复现，不依赖随机环境。
"""

from __future__ import annotations

import random
import tempfile
import unittest
from pathlib import Path

from src.engine import EngineConfig, ExternalJoinEngine
from src.external_sort import SpillConfigurationError
from tests.helpers import read_jsonl, reference_join, write_jsonl

KEY_ALPHABET: list[object] = [
    None, "x", "a", "z", "1", 1, 2, True, False, 1.0, 2.5, "n", None,
]


def _random_rows(rng: random.Random, prefix: str, n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        rows.append(
            {
                "row_id": f"{prefix}{i:03d}",
                "key": rng.choice(KEY_ALPHABET),
                "value": rng.randint(0, 1000),
            }
        )
    return rows


class RandomizedDifferentialTests(unittest.TestCase):
    def test_many_random_inputs_match_reference(self):
        rng = random.Random(20261113)
        scenarios = 0
        for seed_case in range(24):
            rng.seed(20261113 + seed_case)
            left = _random_rows(rng, "l", rng.randint(0, 30))
            right = _random_rows(rng, "r", rng.randint(0, 30))
            join_type = rng.choice(("inner", "left"))
            max_records = rng.choice((1, 2, 3, 5))
            max_bytes = rng.choice((160, 220, 320, 500))
            max_open_files = rng.choice((2, 3, 4, 6))

            tmp = Path(tempfile.mkdtemp())
            left_path, right_path = tmp / "l.jsonl", tmp / "r.jsonl"
            write_jsonl(left_path, left)
            write_jsonl(right_path, right)
            try:
                config = EngineConfig(join_type, max_records, max_bytes, max_open_files)
            except ValueError:
                # 物理不可配置组合（records=1 且 FD<3）应在入口被明确拒绝。
                self.assertEqual((max_records, max_open_files), (1, 2))
                scenarios += 1
                continue
            output = tmp / "out.jsonl"
            try:
                stats = ExternalJoinEngine(
                    left_path, right_path, config, tmp / "run"
                ).run(output)
            except SpillConfigurationError:
                # 数据依赖的物理不可行：FD=2 且确实溢写出多 run。
                # 引擎必须以明确配置错误失败，而不是裸 FD 异常或静默错算。
                self.assertEqual(max_open_files, 2)
                self.assertGreater(len(left) + len(right), max_records)
                scenarios += 1
                continue

            got = read_jsonl(output)
            expected = reference_join(left, right, join_type)
            self.assertEqual(
                got, expected,
                msg=(
                    f"场景 {seed_case} 不一致：join={join_type} "
                    f"records={max_records} bytes={max_bytes} fds={max_open_files} "
                    f"left={len(left)} right={len(right)}"
                ),
            )
            # 多重集基数：inner 等于按键（排除 NULL）的笛卡尔乘积总和。
            if join_type == "inner":
                self.assertEqual(stats["output_rows"], len(expected))
            scenarios += 1
        self.assertEqual(scenarios, 24)


if __name__ == "__main__":
    unittest.main()
