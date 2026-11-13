"""键规范化、记录信封与确定性二进制序列化。

语义契约：
- SQL 普通等号语义：NULL 永远不等于 NULL，NULL 键不参与匹配；
- 严格类型：字符串 "1"、整数 1、布尔 True、浮点 1.0 属于不同类型，互不匹配；
- 规范化键带类型标记，可在 JSON 标量上构成全序，供外排序与最终归并使用；
- 所有溢写行都经过 :func:`encode_line` 编码，字节数可独立复核。
"""

from __future__ import annotations

import json
from typing import Any

# 类型标记同时决定规范化键的全序：NULL < BOOL < INT < FLOAT < STRING
TAG_NULL = 0
TAG_BOOL = 1
TAG_INT = 2
TAG_FLOAT = 3
TAG_STR = 4

TYPE_NAMES = {
    TAG_NULL: "null",
    TAG_BOOL: "bool",
    TAG_INT: "int",
    TAG_FLOAT: "float",
    TAG_STR: "string",
}

# 右 row_id 缺失（left join 未匹配）时的排序标记：0 排在任何真实行之前。
RIGHT_ID_ABSENT = 0
RIGHT_ID_PRESENT = 1


def normalize_key(value: Any) -> tuple[int, Any]:
    """把原始 JSON 键转换为 ``(类型标记, 可比值)``。

    不支持的复合类型（list/dict）直接报错，而不是隐式转字符串。
    """
    if value is None:
        return TAG_NULL, None
    # 注意必须先判 bool，因为 Python 中 bool 是 int 的子类。
    if isinstance(value, bool):
        return TAG_BOOL, int(value)
    if isinstance(value, int):
        return TAG_INT, value
    if isinstance(value, float):
        return TAG_FLOAT, value
    if isinstance(value, str):
        return TAG_STR, value
    raise ValueError(f"不支持的联接键类型: {type(value).__name__}")


def describe_normalized(tag: int, value: Any) -> dict[str, Any]:
    """输出行中面向使用者的键描述。"""
    return {"type": TYPE_NAMES[tag], "value": None if tag == TAG_NULL else value}


def validate_record(record: Any, side: str) -> None:
    """输入行契约校验：必须是对象，row_id 为非空字符串，key 字段存在（可空）。"""
    if not isinstance(record, dict):
        raise ValueError(f"{side} 侧输入记录必须是 JSON 对象，实际为 {type(record).__name__}")
    if "row_id" not in record:
        raise ValueError(f"{side} 侧记录缺少 row_id 字段: {record!r}")
    if not isinstance(record["row_id"], str) or not record["row_id"]:
        raise ValueError(f"{side} 侧 row_id 必须是非空字符串，实际为 {record['row_id']!r}")
    if "key" not in record:
        raise ValueError(f"{side} 侧记录 {record['row_id']} 缺少 key 字段（键可空但字段必须存在）")
    normalize_key(record["key"])  # 提前拒绝不支持的键类型


def record_envelope(side: str, record: dict[str, Any]) -> dict[str, Any]:
    """构造排序阶段的内部信封，保留完整原始记录身份。"""
    validate_record(record, side)
    tag, value = normalize_key(record["key"])
    return {
        "side": side,
        "tag": tag,
        "kval": value,
        "id": record["row_id"],
        # 单侧排序键：规范化键 + row_id，全部为可直接比较的 JSON 标量。
        "sort": [tag, value, record["row_id"]],
        "rec": record,
    }


def output_envelope(left_env: dict[str, Any], right_env: dict[str, Any] | None) -> dict[str, Any]:
    """构造一条联接结果信封。

    right_env 为 None 表示 left join 未匹配，右身份与右 row_id 置空。
    排序键 sk 结构：[键标记, 键值, 左row_id, 右存在标记, 右row_id]，
    其中右存在标记保证空右行稳定排在同键真实匹配之前。
    """
    tag = left_env["tag"]
    value = left_env["kval"]
    if right_env is None:
        right_record = None
        right_id = None
        right_flag = RIGHT_ID_ABSENT
    else:
        right_record = right_env["rec"]
        right_id = right_env["id"]
        right_flag = RIGHT_ID_PRESENT
    out = {
        "normalized_key": describe_normalized(tag, value),
        "left_row_id": left_env["id"],
        "right_row_id": right_id,
        "left": left_env["rec"],
        "right": right_record,
    }
    return {
        "sk": [tag, value, left_env["id"], right_flag, right_id or ""],
        "out": out,
    }


def encode_line(obj: dict[str, Any]) -> bytes:
    """所有溢写文件统一的行编码：紧凑 JSON + LF，UTF-8。

    预算统计中的“序列化字节”与本函数输出字节完全一致，可直接对照文件复核。
    """
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def line_size(obj: dict[str, Any]) -> int:
    return len(encode_line(obj))
