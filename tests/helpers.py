"""测试公共工具：合成输入生成器与小规模内存参考联接。

参考实现刻意朴素（无溢写概念），用于逐行比对外部执行器的有序多重集输出；
二者唯一共享的是 src.normalization 的键规范化规则，避免参考与被测同源实现。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.normalization import RIGHT_ID_ABSENT, normalize_key


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")


def make_hot_key_rows(
    prefix: str,
    key: Any,
    count: int,
    *,
    start: int = 1,
    value_base: int = 0,
) -> list[dict[str, Any]]:
    """生成同一热点键、独立 row_id 的多行。"""
    return [
        {"row_id": f"{prefix}{i}", "key": key, "value": value_base + i}
        for i in range(start, start + count)
    ]


def make_mixed_rows(prefix: str, spec: list[tuple[Any, int]]) -> list[dict[str, Any]]:
    """按 [(键, 行数), ...] 生成多键数据，row_id 全局唯一。"""
    rows: list[dict[str, Any]] = []
    counter = 1
    for key, count in spec:
        for _ in range(count):
            rows.append({"row_id": f"{prefix}{counter}", "key": key, "value": counter})
            counter += 1
    return rows


def _key_sort_value(tag: int, value: Any) -> tuple[Any, ...]:
    return tag, value


def reference_join(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
    join_type: str,
) -> list[dict[str, Any]]:
    """朴素内存参考联接，输出与执行器完全相同的有序负载多重集。"""
    right_index: dict[tuple[int, Any], list[dict[str, Any]]] = {}
    for row in right:
        right_index.setdefault(normalize_key(row["key"]), []).append(row)
    for buckets in right_index.values():
        buckets.sort(key=lambda r: r["row_id"])

    output: list[dict[str, Any]] = []
    for left_row in left:
        nkey = normalize_key(left_row["key"])
        matches: list[dict[str, Any]] = [] if nkey[0] == 0 else right_index.get(nkey, [])
        if matches:
            for right_row in matches:
                output.append(_output_row(left_row, right_row, nkey))
        elif join_type == "left":
            output.append(_output_row(left_row, None, nkey))

    output.sort(
        key=lambda row: (
            _TYPE_TAG[row["normalized_key"]["type"]],
            row["normalized_key"]["value"],
            row["left_row_id"],
            RIGHT_ID_ABSENT if row["right_row_id"] is None else 1,
            row["right_row_id"] or "",
        )
    )
    return output


def _output_row(
    left_row: dict[str, Any],
    right_row: dict[str, Any] | None,
    nkey: tuple[int, Any],
) -> dict[str, Any]:
    tag, value = nkey
    return {
        "normalized_key": {
            "type": {0: "null", 1: "bool", 2: "int", 3: "float", 4: "string"}[tag],
            "value": None if tag == 0 else value,
        },
        "left_row_id": left_row["row_id"],
        "right_row_id": None if right_row is None else right_row["row_id"],
        "left": left_row,
        "right": right_row,
    }


# 类型名 -> 数值标记，排序必须按标记全序（null<bool<int<float<string），
# 不能按类型名字典序（字典序会把 int 排到 float 之后）。
_TYPE_TAG = {"null": 0, "bool": 1, "int": 2, "float": 3, "string": 4}


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
