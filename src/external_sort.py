"""外排序与 FD 受限的多轮 k 路归并。

阶段一（ExternalSorter）：流式读入记录信封，双约束缓冲触顶即排序落 run；
单条超字节预算的记录不进缓冲，逐行写入 oversized 通道，不拆 JSON。
阶段二（merge_runs）：当 run 数超过单轮归并能力时按 fan_in 分轮归并。

预算口径：
- 累积型缓冲（排序缓冲、热点组缓冲）受记录数与序列化字节双硬约束；
- 归并堆每路只持有一个首行，路数受记录预算与 FD 上限硬约束，
  其字节峰值如实统计报告；物理上无法在不放弃推进的前提下保证
  两个各自合规的首行之和也不超字节预算，故字节不对归并堆硬限。
"""

from __future__ import annotations

import heapq
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .budget import BudgetBuffer, BufferStats, FDTracker, OversizedRecord
from .normalization import encode_line, line_size

# 排序信封与输出信封各自的比较键字段。
SORT_KEY = "sort"
OUTPUT_KEY = "sk"


class RunReader:
    """逐行读取 run/chunk 文件，暴露当前信封与其比较键。"""

    def __init__(self, path: Path, fd: FDTracker, key_field: str):
        self._ctx = fd.open(path, "rb")
        self._stream = self._ctx.__enter__()
        self.key_field = key_field
        self.path = path
        self.current: dict[str, Any] | None = None
        self.current_bytes: bytes = b""
        self.advance()

    def advance(self) -> None:
        raw = self._stream.readline()
        if not raw:
            self.current = None
            self.current_bytes = b""
            self.close()
            return
        self.current_bytes = raw
        self.current = json.loads(raw.decode("utf-8"))

    @property
    def key(self) -> list[Any]:
        assert self.current is not None
        return self.current[self.key_field]

    def close(self) -> None:
        if self._stream is not None and not self._stream.closed:
            self._ctx.__exit__(None, None, None)


@dataclass
class MergeStats:
    rounds: int = 0
    peak_input_runs: int = 0
    peak_heap_bytes: int = 0
    merge_written_bytes: int = 0
    merge_written_records: int = 0
    per_round_runs: list[int] = field(default_factory=list)


def _kway_merge(
    runs: list[Path],
    out_path: Path,
    fd: FDTracker,
    key_field: str,
    merge_stats: MergeStats,
    tmp_suffix: str,
) -> None:
    """单轮 k 路归并，runs 数量已由调用方保证不超过 fan_in。"""
    readers: list[RunReader] = []
    heap: list[tuple[list[Any], int, bytes]] = []
    heap_bytes = 0
    tmp_path = out_path.with_name(out_path.name + tmp_suffix)
    try:
        for index, path in enumerate(runs):
            reader = RunReader(path, fd, key_field)
            readers.append(reader)
            if reader.current is not None:
                heapq.heappush(heap, (reader.key, index, reader.current_bytes))
                heap_bytes += len(reader.current_bytes)
        merge_stats.peak_input_runs = max(merge_stats.peak_input_runs, len(heap))
        merge_stats.peak_heap_bytes = max(merge_stats.peak_heap_bytes, heap_bytes)
        written = 0
        written_records = 0
        with fd.open(tmp_path, "wb") as out:
            while heap:
                _, index, raw = heapq.heappop(heap)
                out.write(raw)
                written += len(raw)
                written_records += 1
                heap_bytes -= len(raw)
                reader = readers[index]
                reader.advance()
                if reader.current is not None:
                    heapq.heappush(heap, (reader.key, index, reader.current_bytes))
                    heap_bytes += len(reader.current_bytes)
                    merge_stats.peak_heap_bytes = max(merge_stats.peak_heap_bytes, heap_bytes)
        tmp_path.replace(out_path)
        merge_stats.merge_written_bytes += written
        merge_stats.merge_written_records += written_records
    finally:
        for reader in readers:
            reader.close()


class SpillConfigurationError(ValueError):
    """产生多个 run 但 FD 预算不足以完成外部归并。"""


def merge_runs(
    runs: list[Path],
    out_dir: Path,
    prefix: str,
    fd: FDTracker,
    key_field: str,
    max_records: int,
    max_open_files: int,
) -> tuple[Path | None, MergeStats]:
    """多轮 k 路归并。返回 (最终单个 run 路径, 统计)；无 run 时路径为 None。

    fan_in = min(max_open_files - 1, max(1, max_records))：
    - 减 1 预留给归并输出句柄（k 输入 + 1 输出 <= FD 上限）；
    - max_records=1 时 fan_in=1，单路透传一轮后下一轮两两归并，必然收敛；
    - 字节峰值经 MergeStats.peak_heap_bytes 报告，不做硬性截断。

    物理约束：任意多 run 外部归并都需要一次 2 输入 + 1 输出（共 3 FD）的
    两两归并。max_open_files=2 仅在数据恰好只产生一个 run 时可行；
    一旦真的溢写产生多 run，明确抛出 SpillConfigurationError，而非裸 FD 崩溃。
    """
    stats = MergeStats()
    if not runs:
        return None, stats
    if len(runs) == 1:
        return runs[0], stats
    if max_open_files < 3:
        raise SpillConfigurationError(
            f"已产生 {len(runs)} 个排序 run，但 max_open_files={max_open_files} "
            "不足以做外部归并（至少需要 2 输入 + 1 输出 = 3 个句柄）；"
            "请提高 max_open_files，或放宽记录/字节预算使数据可在单个 run 内完成"
        )
    fan_in = min(max_open_files - 1, max(1, max_records))
    if fan_in < 1:
        raise ValueError("当前预算下无法进行归并：max_open_files 至少为 2")
    current = list(runs)
    round_no = 0
    while len(current) > 1:
        round_no += 1
        stats.rounds += 1
        stats.per_round_runs.append(len(current))
        next_runs: list[Path] = []
        start = 0
        part = 0
        while start < len(current):
            chunk = current[start : start + fan_in]
            if len(chunk) == 1:
                # 单路透传：不打开归并句柄、不复制字节。
                next_runs.append(chunk[0])
            else:
                out_path = out_dir / f"{prefix}.merge.r{round_no}.p{part}.run"
                _kway_merge(chunk, out_path, fd, key_field, stats, ".partial")
                next_runs.append(out_path)
            start += fan_in
            part += 1
        # fan_in=1 时全部透传，数量不变：强制两两归并以保证收敛。
        if len(next_runs) == len(current):
            forced: list[Path] = []
            for i in range(0, len(next_runs), 2):
                pair = next_runs[i : i + 2]
                if len(pair) == 1:
                    forced.append(pair[0])
                    continue
                out_path = out_dir / f"{prefix}.merge.r{round_no}.force{i // 2}.run"
                _kway_merge(pair, out_path, fd, key_field, stats, ".partial")
                forced.append(out_path)
            next_runs = forced
        current = next_runs
    return current[0], stats


class ExternalSorter:
    """流式外排序器：触顶排序落 run，超限行走逐行通道。"""

    def __init__(
        self,
        side: str,
        run_dir: Path,
        max_records: int,
        max_bytes: int,
        fd: FDTracker,
        oversized_path: Path | None = None,
    ):
        self.side = side
        self.run_dir = run_dir
        self.fd = fd
        self.buffer = BudgetBuffer(f"{side}_sort_buffer", max_records, max_bytes)
        self.run_paths: list[Path] = []
        self.oversized_path = oversized_path
        self.oversized_records: list[dict[str, Any]] = []
        self._run_seq = 0

    @property
    def stats(self) -> BufferStats:
        return self.buffer.stats

    def add(self, env: dict[str, Any]) -> None:
        size = line_size(env)
        if size > self.buffer.max_bytes:
            # 单行即超字节预算：逐行通道，每次打开即关闭，不长期占用 FD。
            exc = OversizedRecord(self.side, env["id"], size, self.buffer.max_bytes)
            self.oversized_records.append(
                {"side": self.side, "row_id": env["id"], "size": size, "reason": str(exc)}
            )
            if self.oversized_path is None:
                raise RuntimeError("未配置 oversized 通道但收到超大记录")
            with self.fd.open(self.oversized_path, "ab") as stream:
                stream.write(encode_line(env))
            return
        # 加入前预判：保证缓冲峰值最多等于预算，不会因再收一条而越界。
        if self.buffer.would_overflow(env):
            self.flush()
        accepted = self.buffer.add(env)
        assert accepted  # 单行不超限，预判后必然收下

    def flush(self) -> Path | None:
        items = self.buffer.drain()
        if not items:
            return None
        items.sort(key=lambda item: item[SORT_KEY])
        self._run_seq += 1
        final_path = self.run_dir / f"{self.side}.run{self._run_seq:05d}.jsonl"
        tmp_path = final_path.with_name(final_path.name + ".partial")
        with self.fd.open(tmp_path, "wb") as stream:
            for item in items:
                stream.write(encode_line(item))
        tmp_path.replace(final_path)  # 原子改名，崩溃后只留 .partial，不会被当成完成 run
        self.run_paths.append(final_path)
        return final_path

    def finish(self) -> None:
        self.flush()
