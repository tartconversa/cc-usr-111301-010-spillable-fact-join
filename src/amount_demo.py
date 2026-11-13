"""金额聚合演示：显式揭示多对多联接的金额扩张，绝不自动平摊。

业务背景（README 同述）：资产维度与计费明细在共享机房键上都是多行，
联接结果是多重集。若直接对右表金额求和，同一金额会按左表重复次数被
重复计入；若按左表平摊（amount / 左重复数）则是在替业务做未授权的
会计假设。本工具只做事实报告：扩张倍数、扩张前后金额与每个多对多键。
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from .join_reader import load
from .normalization import normalize_key


def _key_index(rows: list[dict[str, Any]]) -> dict[tuple[int, Any], list[dict[str, Any]]]:
    index: dict[tuple[int, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        index[normalize_key(row["key"])].append(row)
    return index


def amount_expansion_report(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> dict[str, Any]:
    """返回多对多扩张诊断报告。NULL 键不参与匹配（与执行器语义一致）。"""
    left_index = _key_index(left)
    right_index = _key_index(right)
    per_key: list[dict[str, Any]] = []
    join_rows = 0
    expanded_amount = 0
    raw_right_amount = 0

    for key, right_rows in right_index.items():
        if key[0] == 0:  # NULL 键跳过
            continue
        raw_right_amount += sum(int(r["value"]) for r in right_rows)
        left_rows = left_index.get(key, [])
        if not left_rows:
            continue
        pairs = len(left_rows) * len(right_rows)
        join_rows += pairs
        key_amount = sum(int(r["value"]) for r in right_rows) * len(left_rows)
        expanded_amount += key_amount
        if len(left_rows) > 1 and len(right_rows) > 1:
            cardinality = "many_to_many"
        elif len(left_rows) > 1 or len(right_rows) > 1:
            cardinality = "one_to_many"
        else:
            cardinality = "one_to_one"
        per_key.append(
            {
                "key": None if key[0] == 0 else key[1],
                "left_rows": len(left_rows),
                "right_rows": len(right_rows),
                "join_rows": pairs,
                "right_amount_raw": sum(int(r["value"]) for r in right_rows),
                "right_amount_after_join": key_amount,
                "expansion_factor": len(left_rows),
                "cardinality": cardinality,
                "warning": (
                    "多对多键：右表金额在联接后被复制，禁止直接求和或自动平摊，"
                    "须先在各自粒度汇总并由业务确认分摊规则"
                    if cardinality == "many_to_many"
                    else None
                ),
            }
        )

    per_key.sort(key=lambda item: (item["key"] is None, item["key"]))
    return {
        "join_type_semantics": "inner，NULL 键不匹配；结果为多重集，不去重",
        "total_join_rows": join_rows,
        "raw_right_amount": raw_right_amount,
        "naive_sum_after_join": expanded_amount,
        "global_expansion_factor": (
            expanded_amount / raw_right_amount if raw_right_amount else None
        ),
        "proration_policy": "不自动平摊：任何分摊必须由业务规则显式指定",
        "per_key": per_key,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="多对多联接金额扩张诊断（只读小输入，不落盘）")
    parser.add_argument("--left", required=True, type=Path)
    parser.add_argument("--right", required=True, type=Path)
    args = parser.parse_args(argv)
    report = amount_expansion_report(load(args.left), load(args.right))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
