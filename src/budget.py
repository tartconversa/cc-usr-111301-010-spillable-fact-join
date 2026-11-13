"""可核验的双约束内存预算与文件描述符跟踪。

预算口径（README 中有同样说明，便于复核）：
- 任一受控缓冲（外排序运行缓冲、归并输入堆、热点组分块缓冲、交叉块缓冲）
  同时受 *批记录数* 与 *序列化字节* 双约束，任一达到上限即触发溢写；
- 字节按 src.normalization.encode_line 的实际行编码统计，可与磁盘上的
  run/chunk 文件逐字节对照；
- 单行本身超限不挤爆缓冲，转入专门的逐行通道，并计入 OversizedRecord 例外；
- FD 跟踪器包装所有引擎内 open()，峰值不得超过 max_open_files 配置。
"""

from __future__ import annotations

import builtins
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

from .normalization import line_size


class OversizedRecord(RuntimeError):
    """单行序列化后即超过记录批次预算的例外（走专门逐行通道）。"""

    def __init__(self, side: str, row_id: str, size: int, max_bytes: int):
        super().__init__(
            f"{side} 侧记录 {row_id} 序列化 {size} 字节，超过批次字节预算 {max_bytes}，"
            "转入单记录逐行通道"
        )
        self.side = side
        self.row_id = row_id
        self.size = size
        self.max_bytes = max_bytes


@dataclass
class BufferStats:
    """单个受控缓冲的峰值与溢写统计。"""

    name: str
    peak_records: int = 0
    peak_bytes: int = 0
    spill_count: int = 0
    spill_bytes: int = 0
    spill_records: int = 0

    def record_spill(self, records: int, bytes_: int) -> None:
        self.spill_count += 1
        self.spill_records += records
        self.spill_bytes += bytes_


class BudgetBuffer:
    """双约束批次缓冲。

    add 返回值约定：
    - True：已收下（缓冲可能因此触顶，调用方应随后检查 full）；
    - False：单行本身超过字节预算，调用方必须走逐行通道，缓冲不被污染。
    """

    def __init__(self, name: str, max_records: int, max_bytes: int):
        if max_records < 1:
            raise ValueError("buffer_max_records 必须 >= 1")
        if max_bytes < 1:
            raise ValueError("buffer_max_serialized_bytes 必须 >= 1")
        self.name = name
        self.max_records = max_records
        self.max_bytes = max_bytes
        self.items: list[dict[str, Any]] = []
        self.bytes = 0
        self.stats = BufferStats(name=name)

    def add(self, obj: dict[str, Any]) -> bool:
        size = line_size(obj)
        if size > self.max_bytes:
            return False
        self.items.append(obj)
        self.bytes += size
        self.stats.peak_records = max(self.stats.peak_records, len(self.items))
        self.stats.peak_bytes = max(self.stats.peak_bytes, self.bytes)
        return True

    @property
    def full(self) -> bool:
        return len(self.items) >= self.max_records or self.bytes >= self.max_bytes

    def would_overflow(self, obj: dict[str, Any]) -> bool:
        """预判：在当前缓冲上追加 obj 是否会突破任一预算（obj 本身不超限）。"""
        return (
            len(self.items) + 1 > self.max_records
            or self.bytes + line_size(obj) > self.max_bytes
        )

    def drain(self) -> list[dict[str, Any]]:
        """取走全部内容并复位，取走的体量计入溢写统计（真实落盘时使用）。"""
        items = self.items
        if items:
            self.stats.record_spill(len(items), self.bytes)
        self.items = []
        self.bytes = 0
        return items

    def take(self) -> list[dict[str, Any]]:
        """取走全部内容并复位，但不计溢写（内存内整组消费时使用）。

        峰值已在 add 中记录，统计口径不受影响。
        """
        items = self.items
        self.items = []
        self.bytes = 0
        return items

    def __len__(self) -> int:
        return len(self.items)


class FDTracker:
    """对引擎所有 open() 计数，硬性保证并发句柄数不超过配置。"""

    def __init__(self, max_open_files: int):
        self.max_open_files = max_open_files
        self.open_count = 0
        self.peak = 0

    @contextmanager
    def open(self, path: Path | str, mode: str = "r", **kwargs: Any) -> Iterator[IO[Any]]:
        if self.open_count >= self.max_open_files:
            raise RuntimeError(
                f"文件描述符预算耗尽：已开 {self.open_count}，上限 {self.max_open_files}"
            )
        stream = builtins.open(path, mode, **kwargs)
        self.open_count += 1
        self.peak = max(self.peak, self.open_count)
        try:
            yield stream
        finally:
            stream.close()
            self.open_count -= 1
