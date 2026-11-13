"""有界内存外部联接执行引擎。

三阶段流水线（每阶段有显式清单标记，可独立恢复）：
  阶段1 双侧外排序：双约束缓冲溢写 run，FD 受限多轮归并为单 run；
                    超大单行进入逐行通道，记录 OversizedRecord 例外；
  阶段2 排序归并联接：按规范化键流式归组，普通组内存交叉；
                    超过预算的热点组两侧各切成磁盘 chunk，分块交叉，
                    笛卡尔块按全局序号写入 parts 清单，重启不重复追加；
  阶段3 输出归并与提交：三个联接片段按输出全序多轮归并，校验后原子提交。

NULL 键在阶段2 被直接跳过（SQL 普通等号语义）；left join 的未匹配
（含 NULL 键左行）输出右为空，并在输出全序中排在同键匹配之前。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .budget import BudgetBuffer, FDTracker, OversizedRecord
from .external_sort import (
    OUTPUT_KEY,
    SORT_KEY,
    ExternalSorter,
    RunReader,
    merge_runs,
)
from .normalization import encode_line, line_size, output_envelope, record_envelope

STAGE1_MARKER = "stage1.done"
STAGE2_MARKER = "stage2.done"
STAGE3_MARKER = "stage3.done"
FINGERPRINT_NAME = "fingerprint.json"

# 四个联接片段对应左/右两侧“正常 / 超大”划分的四个互不相交矩阵单元格，
# 合起来恰好覆盖完整多重集且无重复：
#   主片段 正常×正常；超大片段B 超大左×正常右；
#   超大片段C 正常左×超大右；超大片段D 超大左×超大右。
# 阶段3 按此固定顺序汇总，最终再做一次全局有序归并消除任何边界重叠。
FRAG_MAIN = "frag-main"
FRAG_OVERLEFT = "frag-over-left"
FRAG_OVERRIGHT = "frag-over-right"
FRAG_OVERBOTH = "frag-over-both"
FRAGMENT_ORDER = (FRAG_MAIN, FRAG_OVERLEFT, FRAG_OVERRIGHT, FRAG_OVERBOTH)


def journal_name(fragment: str) -> str:
    return f"stage2-parts.{fragment}.jsonl"


def fragment_done_name(fragment: str) -> str:
    return f"stage2-parts.{fragment}.done"


class InjectedCrash(RuntimeError):
    """故障注入点触发的模拟崩溃。"""


class RunReuseError(RuntimeError):
    """输入字节哈希或执行参数变化时拒绝复用旧 run。"""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass
class EngineConfig:
    join_type: str  # "inner" 或 "left"
    max_records: int
    max_bytes: int
    max_open_files: int
    fault: str | None = None

    def __post_init__(self) -> None:
        if self.join_type not in ("inner", "left"):
            raise ValueError("join_type 仅支持 inner 或 left")
        if self.max_records < 1:
            raise ValueError("buffer_max_records 必须 >= 1")
        if self.max_bytes < 1:
            raise ValueError("buffer_max_serialized_bytes 必须 >= 1")
        if self.max_open_files < 2:
            raise ValueError("max_open_files 至少为 2（归并需 1 输入 + 1 输出句柄）")
        # 记录批次为 1 时，fan_in 只能取 1，最终收敛依赖一次两两归并，
        # 该归并需要 2 个输入句柄 + 1 个输出句柄 = 3 个 FD，少于 3 无法推进。
        if self.max_records == 1 and self.max_open_files < 3:
            raise ValueError(
                "buffer_max_records=1 时 max_open_files 至少为 3："
                "单路透传后的两两归并需要 2 输入 + 1 输出句柄"
            )

    def fingerprint_params(self) -> dict[str, Any]:
        # fault 是测试设施，不参与复用指纹（同一执行在注入/不注入下可恢复）。
        return {
            "join_type": self.join_type,
            "max_records": self.max_records,
            "max_bytes": self.max_bytes,
            "max_open_files": self.max_open_files,
        }


@dataclass
class GroupBufferStats:
    """热点组缓冲的聚合统计（左右两侧合并口径）。"""

    peak_records: int = 0
    peak_bytes: int = 0
    spill_count: int = 0
    spill_bytes: int = 0
    spill_records: int = 0
    hot_groups: int = 0
    hot_chunk_files: int = 0
    oversized_singleton_chunks: int = 0

    def absorb_buffer(self, buf: BudgetBuffer) -> None:
        self.peak_records = max(self.peak_records, buf.stats.peak_records)
        self.peak_bytes = max(self.peak_bytes, buf.stats.peak_bytes)
        self.spill_count += buf.stats.spill_count
        self.spill_bytes += buf.stats.spill_bytes
        self.spill_records += buf.stats.spill_records


class _PartJournal:
    """阶段2/3 的 parts 清单：仅接受原子写入后的块，条目含字节哈希。"""

    def __init__(self, path: Path, fd: FDTracker):
        self.path = path
        self.fd = fd

    def entries(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        result: list[dict[str, Any]] = []
        with self.fd.open(self.path, "rb") as stream:
            for line in stream:
                result.append(json.loads(line.decode("utf-8")))
        return result

    def append(self, entry: dict[str, Any]) -> None:
        with self.fd.open(self.path, "ab") as stream:
            stream.write(encode_line(entry))
            stream.flush()

    def verify_entry(self, entry: dict[str, Any], run_dir: Path) -> bool:
        part_path = run_dir / entry["part"]
        return (
            part_path.exists()
            and sha256_file(part_path) == entry["sha256"]
            and part_path.stat().st_size == entry["bytes"]
        )


class ExternalJoinEngine:
    def __init__(
        self,
        left_path: Path,
        right_path: Path,
        config: EngineConfig,
        run_dir: Path,
    ):
        if config.join_type not in ("inner", "left"):
            raise ValueError("join_type 仅支持 inner 或 left")
        self.left_path = left_path
        self.right_path = right_path
        self.config = config
        self.run_dir = run_dir
        self.fd = FDTracker(config.max_open_files)
        self._part_seq = 0
        self._gen = 0
        self._fragment: str | None = None
        self.journal: _PartJournal | None = None
        self.stats: dict[str, Any] = {
            "input": {},
            "sort": {},
            "merge_rounds": {},
            "hot_groups": {},
            "parts": 0,
            "output_rows": 0,
            "output_bytes": 0,
            "oversized": [],
            "fd_peak": 0,
            "injected_fault": config.fault,
        }
        self.group_stats = GroupBufferStats()

    # ------------------------------------------------------------------ 工具
    def _crash(self, label: str) -> None:
        if self.config.fault == label:
            raise InjectedCrash(f"故障注入点 {label} 触发")

    def _next_part_name(self) -> str:
        assert self._fragment is not None
        # 片段内序号从零开始；片段整体重建，文件名带 rebuild 代号，
        # 旧一代孤儿块可与新一代明确区分并清理。
        name = f"{self._fragment}.gen{self._gen:02d}.part-{self._part_seq:06d}.jsonl"
        self._part_seq += 1
        return name

    def _write_part(self, lines: list[bytes]) -> tuple[str, dict[str, Any]]:
        """原子写一个输出块并登记当前片段清单，返回 (文件名, 清单条目)。"""
        assert self.journal is not None
        name = self._next_part_name()
        final_path = self.run_dir / name
        tmp_path = final_path.with_name(name + ".partial")
        with self.fd.open(tmp_path, "wb") as stream:
            for line in lines:
                stream.write(line)
        tmp_path.replace(final_path)
        entry = {
            "seq": self._part_seq - 1,
            "part": name,
            "sha256": sha256_file(final_path),
            "bytes": final_path.stat().st_size,
            "rows": len(lines),
        }
        self.journal.append(entry)
        self._crash("stage2_after_part")
        return name, entry

    def _marker(self, name: str) -> Path:
        return self.run_dir / name

    def _write_marker(self, name: str, payload: dict[str, Any]) -> None:
        marker = self._marker(name)
        tmp = marker.with_name(name + ".partial")
        with self.fd.open(tmp, "wb") as stream:
            stream.write(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
        tmp.replace(marker)

    def _read_marker(self, name: str) -> dict[str, Any] | None:
        marker = self._marker(name)
        if not marker.exists():
            return None
        return json.loads(marker.read_text(encoding="utf-8"))

    # ------------------------------------------------------------- 指纹/恢复
    def fingerprint(self) -> dict[str, Any]:
        return {
            "inputs": {
                "left": {
                    "path": str(self.left_path),
                    "sha256": sha256_file(self.left_path),
                    "bytes": self.left_path.stat().st_size,
                },
                "right": {
                    "path": str(self.right_path),
                    "sha256": sha256_file(self.right_path),
                    "bytes": self.right_path.stat().st_size,
                },
            },
            "params": self.config.fingerprint_params(),
        }

    def prepare_run_dir(self, force: bool) -> dict[str, Any] | None:
        """校验或初始化 run 目录；指纹变化且无 force 时拒绝复用。

        返回旧指纹（用于判断是否处于恢复场景），全新目录返回 None。
        """
        fp_path = self.run_dir / FINGERPRINT_NAME
        current = self.fingerprint()
        if self.run_dir.exists() and any(self.run_dir.iterdir()):
            if not fp_path.exists():
                raise RunReuseError(
                    f"run 目录 {self.run_dir} 非空且缺少指纹文件，拒绝复用；请更换目录或显式 --force"
                )
            old = json.loads(fp_path.read_text(encoding="utf-8"))
            if old != current:
                if not force:
                    raise RunReuseError(
                        "输入字节哈希或执行参数与旧 run 不一致，拒绝复用；"
                        "确认无误后可使用 --force 清理重建"
                    )
                for child in self.run_dir.iterdir():
                    if child.is_file():
                        child.unlink()
                    else:
                        for nested in child.iterdir():
                            nested.unlink()
                        child.rmdir()
                old = None
        else:
            self.run_dir.mkdir(parents=True, exist_ok=True)
            old = None
        with self.fd.open(fp_path, "wb") as stream:
            stream.write(json.dumps(current, ensure_ascii=False, indent=2).encode("utf-8"))
        return old

    def _fragment_journal(self, fragment: str) -> _PartJournal:
        return _PartJournal(self.run_dir / journal_name(fragment), self.fd)

    def _fragment_complete(self, fragment: str) -> bool:
        """片段完成标记存在且清单每个块字节哈希校验通过，才视为可复用。

        仅有清单而无完成标记，说明上个进程在片段中途崩溃，必须整体重建。
        """
        done_marker = self.run_dir / fragment_done_name(fragment)
        if not done_marker.exists():
            return False
        journal = self._fragment_journal(fragment)
        # 空片段（如 inner 无匹配）同样合法：完成标记 + 可通过校验的清单（可为空）。
        return all(
            journal.verify_entry(entry, self.run_dir)
            for entry in journal.entries()
        )

    def _begin_fragment(self, fragment: str) -> bool:
        """准备片段执行上下文。

        返回 True 表示片段已完整、可直接跳过；False 表示开始一次全新重建：
        删除该片段旧清单、完成标记与所有旧代 part/chunk，序号从 0 重新确定。
        """
        if self._fragment_complete(fragment):
            return True
        journal_path = self.run_dir / journal_name(fragment)
        if journal_path.exists():
            journal_path.unlink()
        done_marker = self.run_dir / fragment_done_name(fragment)
        if done_marker.exists():
            done_marker.unlink()
        for stale in self.run_dir.glob(f"{fragment}.gen*.part-*.jsonl"):
            stale.unlink()
        chunk_dir = self.run_dir / f"chunks-{fragment}"
        if chunk_dir.exists():
            for stale in chunk_dir.glob("*.jsonl"):
                stale.unlink()
        self._gen += 1
        self._fragment = fragment
        self.journal = self._fragment_journal(fragment)
        self._part_seq = 0
        return False

    def _end_fragment(self) -> None:
        # 仅在片段完整扫描结束后写完成标记；中途崩溃不会留下该文件。
        assert self._fragment is not None
        marker = self.run_dir / fragment_done_name(self._fragment)
        entries = self._fragment_journal(self._fragment).entries()
        tmp = marker.with_name(marker.name + ".partial")
        with self.fd.open(tmp, "wb") as stream:
            stream.write(
                json.dumps({"parts": len(entries)}, ensure_ascii=False).encode("utf-8")
            )
        tmp.replace(marker)
        self._fragment = None
        self.journal = None
        self._part_seq = 0

    # --------------------------------------------------------------- 阶段1
    def _iter_input(self, path: Path, side: str) -> Iterator[dict[str, Any]]:
        with self.fd.open(path, "rb") as stream:
            for line_no, raw in enumerate(stream, start=1):
                if not raw.strip():
                    continue
                try:
                    record = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{side} 侧输入第 {line_no} 行不是合法 JSON，禁止丢弃") from exc
                yield record_envelope(side, record)

    def _run_stage1(self) -> dict[str, Path]:
        marker = self._read_marker(STAGE1_MARKER)
        if marker is not None:
            runs = marker["runs"]
            # 恢复时核验清单中所有 run 与超大通道文件。
            for key in ("left_run", "right_run", "left_oversized", "right_oversized"):
                rel = runs[key]
                if rel is not None and not (self.run_dir / rel).exists():
                    marker = None
                    break
            if marker is not None:
                self.stats["input"] = marker["input_stats"]
                self.stats["sort"] = marker["sort_stats"]
                self.stats["merge_rounds"] = marker["merge_rounds"]
                self.stats["oversized"] = marker["oversized"]
                return {key: (self.run_dir / rel if rel else None) for key, rel in runs.items()}

        input_stats: dict[str, Any] = {}
        sort_stats: dict[str, Any] = {}
        merge_rounds: dict[str, Any] = {}
        oversized_log: list[dict[str, Any]] = []
        results: dict[str, Path | None] = {}

        for side, path in (("left", self.left_path), ("right", self.right_path)):
            over_path = self.run_dir / f"{side}.oversized.jsonl"
            sorter = ExternalSorter(
                side,
                self.run_dir,
                self.config.max_records,
                self.config.max_bytes,
                self.fd,
                oversized_path=over_path,
            )
            count = 0
            key_counts: dict[str, int] = {}
            for env in self._iter_input(path, side):
                sorter.add(env)
                count += 1
                key_label = f"{env['tag']}:{env['kval']!r}"
                key_counts[key_label] = key_counts.get(key_label, 0) + 1
            sorter.finish()
            oversized_log.extend(sorter.oversized_records)
            input_stats[side] = {
                "records": count,
                "bytes": path.stat().st_size,
                "distinct_keys": len(key_counts),
            }
            sort_stats[side] = {
                "runs_written": len(sorter.run_paths),
                "peak_records": sorter.stats.peak_records,
                "peak_bytes": sorter.stats.peak_bytes,
                "spill_count": sorter.stats.spill_count,
                "spill_records": sorter.stats.spill_records,
                "spill_bytes": sorter.stats.spill_bytes,
            }
            merged, mstats = merge_runs(
                sorter.run_paths,
                self.run_dir,
                side,
                self.fd,
                SORT_KEY,
                self.config.max_records,
                self.config.max_open_files,
            )
            merge_rounds[side] = {
                "rounds": mstats.rounds,
                "per_round_input_runs": mstats.per_round_runs,
                "peak_input_runs": mstats.peak_input_runs,
                "peak_heap_bytes": mstats.peak_heap_bytes,
                "written_bytes": mstats.merge_written_bytes,
            }
            results[f"{side}_run"] = merged
            results[f"{side}_oversized"] = over_path if over_path.exists() else None
            self._crash("stage1_after_sort")

        marker_payload = {
            "runs": {key: (str(path.relative_to(self.run_dir)) if path else None) for key, path in results.items()},
            "input_stats": input_stats,
            "sort_stats": sort_stats,
            "merge_rounds": merge_rounds,
            "oversized": oversized_log,
        }
        self._write_marker(STAGE1_MARKER, marker_payload)
        self.stats["input"] = input_stats
        self.stats["sort"] = sort_stats
        self.stats["merge_rounds"] = merge_rounds
        self.stats["oversized"] = oversized_log
        return results  # type: ignore[return-value]

    # --------------------------------------------------------------- 阶段2
    def _drain_group(
        self,
        reader: RunReader,
        side: str,
        chunk_dir: Path,
        seq_holder: dict[str, int],
    ) -> tuple[list[dict[str, Any]], list[Path], bool]:
        """读取当前同键整组：放得下则留内存，放不下则切磁盘 chunk。

        返回 (内存信封, 磁盘 chunk 路径, 累积缓冲是否真实触顶溢写)。
        超大行各自写成单例磁盘 chunk，但不置位缓冲溢写标记，因此
        “热点组”统计严格对应双约束累积缓冲触顶，而非仅出现超大行。
        """
        key = reader.key[:2]
        buf = BudgetBuffer(f"hot_group_{side}", self.config.max_records, self.config.max_bytes)
        chunks: list[Path] = []
        in_memory: list[dict[str, Any]] = []
        buffer_spilled = False
        used_chunks = False
        while reader.current is not None and reader.key[:2] == key:
            env = reader.current
            if line_size(env) > self.config.max_bytes:
                # 超大行：先落盘已积累的普通缓冲，再把该行写成单例 chunk。
                if buf.items:
                    self._flush_group_chunk(buf, side, chunk_dir, seq_holder, chunks)
                    buffer_spilled = True
                    used_chunks = True
                self._flush_singleton_chunk(env, side, chunk_dir, seq_holder, chunks)
                used_chunks = True
                reader.advance()
                continue
            # 先预判：仅当当前组再加一条就会突破预算时才切 chunk，
            # 恰好等于预算的组仍可整体留在内存。
            if buf.would_overflow(env):
                self._flush_group_chunk(buf, side, chunk_dir, seq_holder, chunks)
                buffer_spilled = True
                used_chunks = True
            if not buf.add(env):  # pragma: no cover - 上面已按行尺寸分流
                raise OversizedRecord(side, env["id"], 0, self.config.max_bytes)
            reader.advance()
        if buffer_spilled and buf.items:
            self._flush_group_chunk(buf, side, chunk_dir, seq_holder, chunks)
            used_chunks = True
        if buffer_spilled:
            self._crash("stage2_after_hot_chunk")
        if not used_chunks:
            in_memory = buf.take()  # 内存路径：峰值已记，不计溢写
        else:
            # 发生过分块：残余缓冲（无则空）也不应再当作内存整组。
            in_memory = buf.take()
        self.group_stats.absorb_buffer(buf)
        return in_memory, chunks, buffer_spilled

    def _flush_singleton_chunk(
        self,
        env: dict[str, Any],
        side: str,
        chunk_dir: Path,
        seq_holder: dict[str, int],
        chunks: list[Path],
    ) -> None:
        """把单条超大行写成独立 chunk；不计入累积缓冲的双约束统计。"""
        seq_holder[side] = seq_holder.get(side, 0) + 1
        name = f"{side}.chunk{seq_holder[side]:05d}.jsonl"
        final_path = chunk_dir / name
        tmp_path = final_path.with_name(name + ".partial")
        with self.fd.open(tmp_path, "wb") as stream:
            stream.write(encode_line(env))
        tmp_path.replace(final_path)
        chunks.append(final_path)
        self.group_stats.hot_chunk_files += 1
        self.group_stats.oversized_singleton_chunks += 1

    def _flush_group_chunk(
        self,
        buf: BudgetBuffer,
        side: str,
        chunk_dir: Path,
        seq_holder: dict[str, int],
        chunks: list[Path],
    ) -> None:
        items = buf.drain()
        seq_holder[side] = seq_holder.get(side, 0) + 1
        name = f"{side}.chunk{seq_holder[side]:05d}.jsonl"
        final_path = chunk_dir / name
        tmp_path = final_path.with_name(name + ".partial")
        with self.fd.open(tmp_path, "wb") as stream:
            for item in items:
                stream.write(encode_line(item))
        tmp_path.replace(final_path)
        chunks.append(final_path)
        self.group_stats.hot_chunk_files += 1

    def _cross_buffered(
        self,
        left_items: list[dict[str, Any]],
        right_items: list[dict[str, Any]],
    ) -> list[bytes]:
        """两侧均为内存组时的笛卡尔交叉，输出天然按 (左id, 右id) 有序。"""
        lines: list[bytes] = []
        for left_env in left_items:
            for right_env in right_items:
                lines.append(encode_line(output_envelope(left_env, right_env)))
        return lines

    def _cross_chunked(
        self,
        left_mem: list[dict[str, Any]],
        left_chunks: list[Path],
        right_mem: list[dict[str, Any]],
        right_chunks: list[Path],
        chunk_dir: Path,
    ) -> None:
        """热点组磁盘分块交叉：逐块嵌套循环，块内仍受同一预算约束。

        超大行已在 _drain_group 写成单例 chunk，因此这里只需统一处理
        内存源与磁盘 chunk 源；每个块对产出一个 part，片段重建时序号
        重新确定，配对不重不漏。
        """
        left_sources = left_chunks if left_chunks else [None]
        right_sources = right_chunks if right_chunks else [None]
        for left_src in left_sources:
            left_items = left_mem if left_src is None else self._read_chunk(left_src)
            for right_src in right_sources:
                right_items = right_mem if right_src is None else self._read_chunk(right_src)
                self._write_part(self._cross_buffered(left_items, right_items))

    def _read_chunk(self, path: Path) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        with self.fd.open(path, "rb") as stream:
            for raw in stream:
                items.append(json.loads(raw.decode("utf-8")))
        return items

    def _run_join_fragment(
        self,
        left_run: Path | None,
        right_run: Path | None,
        fragment: str,
        emit_unmatched: bool,
    ) -> None:
        """对一对已排序 run 执行归并联接，输出 part 序列到片段私有清单。

        emit_unmatched 区分 inner(False)/left(True)。
        空侧：inner 无输出；left 时左行全部以右空输出。
        """
        if left_run is None:
            return
        # 片段级原子性：完整片段直接跳过，否则整体重建，杜绝部分块重复追加。
        if self._begin_fragment(fragment):
            return
        left_reader = RunReader(left_run, self.fd, SORT_KEY)
        right_reader = (
            RunReader(right_run, self.fd, SORT_KEY) if right_run is not None else None
        )
        chunk_dir = self.run_dir / f"chunks-{fragment}"
        chunk_dir.mkdir(exist_ok=True)
        seq_holder: dict[str, int] = {}
        completed = False
        try:
            while left_reader.current is not None:
                lkey = left_reader.key[:2]
                # NULL 键：SQL 普通等号下不匹配任何行。
                if lkey[0] == 0:
                    if emit_unmatched:
                        self._emit_unmatched_group(left_reader)
                    else:
                        self._skip_group(left_reader)
                    continue
                # 跳过所有键值更小的右键组（包括 NULL 组）。
                while right_reader is not None and right_reader.current is not None and right_reader.key[:2] < lkey:
                    self._skip_group(right_reader)
                right_match = (
                    right_reader is not None
                    and right_reader.current is not None
                    and right_reader.key[:2] == lkey
                )
                if not right_match:
                    if emit_unmatched:
                        self._emit_unmatched_group(left_reader)
                    else:
                        self._skip_group(left_reader)
                    continue
                left_mem, left_chunks, left_hot = self._drain_group(
                    left_reader, "left", chunk_dir, seq_holder
                )
                right_mem, right_chunks, right_hot = self._drain_group(
                    right_reader, "right", chunk_dir, seq_holder
                )
                if left_chunks or right_chunks:
                    # 热点组严格定义为双约束累积缓冲触顶；仅超大单例不计热点。
                    if left_hot or right_hot:
                        self.group_stats.hot_groups += 1
                    self._cross_chunked(
                        left_mem, left_chunks,
                        right_mem, right_chunks,
                        chunk_dir,
                    )
                else:
                    self._write_part(self._cross_buffered(left_mem, right_mem))
            completed = True
        finally:
            left_reader.close()
            if right_reader is not None:
                right_reader.close()
            if completed:
                # 仅完整扫描后写完成标记；崩溃路径不写，恢复时整体重建。
                self._end_fragment()
            else:
                self._fragment = None
                self.journal = None
                self._part_seq = 0

    def _skip_group(self, reader: RunReader) -> None:
        key = reader.key[:2]
        while reader.current is not None and reader.key[:2] == key:
            reader.advance()

    def _emit_unmatched_group(self, left_reader: RunReader) -> None:
        key = left_reader.key[:2]
        batch: list[bytes] = []
        while left_reader.current is not None and left_reader.key[:2] == key:
            env = left_reader.current
            batch.append(encode_line(output_envelope(env, None)))
            left_reader.advance()
            # 未匹配批也遵守记录数预算，拆成多个 part 流式落盘。
            if len(batch) >= self.config.max_records:
                self._write_part(batch)
                batch = []
        if batch:
            self._write_part(batch)

    def _run_stage2(self, stage1: dict[str, Path]) -> None:
        if self._read_marker(STAGE2_MARKER) is not None:
            return

        # 主片段：正常 run 对正常 run。
        self._run_join_fragment(
            stage1["left_run"], stage1["right_run"], FRAG_MAIN,
            emit_unmatched=self.config.join_type == "left",
        )
        # 片段B：超大左行 × 全部右行（右 NULL 键自然不匹配，由联接逻辑处理）。
        if stage1["left_oversized"] is not None:
            self._run_join_fragment(
                stage1["left_oversized"], stage1["right_run"], FRAG_OVERLEFT,
                emit_unmatched=self.config.join_type == "left",
            )
        # 片段C：正常左行 × 超大右行；超大右行为 NULL 时 inner 无输出、left 不受影响。
        if stage1["right_oversized"] is not None:
            self._run_join_fragment(
                stage1["left_run"], stage1["right_oversized"], FRAG_OVERRIGHT,
                emit_unmatched=False,
            )
        # 片段D：超大左 × 超大右，补齐矩阵最后一个单元格。
        if stage1["left_oversized"] is not None and stage1["right_oversized"] is not None:
            self._run_join_fragment(
                stage1["left_oversized"], stage1["right_oversized"], FRAG_OVERBOTH,
                emit_unmatched=False,
            )

        total_parts = 0
        total_rows = 0
        for fragment in FRAGMENT_ORDER:
            entries = self._fragment_journal(fragment).entries()
            total_parts += len(entries)
            total_rows += sum(entry["rows"] for entry in entries)
        self._write_marker(STAGE2_MARKER, {"parts": total_parts, "rows": total_rows})
        self.stats["parts"] = total_parts
        self.stats["hot_groups"] = {
            "hot_groups": self.group_stats.hot_groups,
            "hot_chunk_files": self.group_stats.hot_chunk_files,
            "oversized_singleton_chunks": self.group_stats.oversized_singleton_chunks,
            "peak_records": self.group_stats.peak_records,
            "peak_bytes": self.group_stats.peak_bytes,
            "spill_count": self.group_stats.spill_count,
            "spill_records": self.group_stats.spill_records,
            "spill_bytes": self.group_stats.spill_bytes,
        }

    # --------------------------------------------------------------- 阶段3
    def _run_stage3(self, output_path: Path) -> None:
        if self._read_marker(STAGE3_MARKER) is not None:
            marker = self._read_marker(STAGE3_MARKER)
            assert marker is not None
            self.stats["output_rows"] = marker["rows"]
            self.stats["output_bytes"] = marker["bytes"]
            return
        part_paths: list[Path] = []
        for fragment in FRAGMENT_ORDER:
            for entry in self._fragment_journal(fragment).entries():
                part_paths.append(self.run_dir / entry["part"])
        merged, mstats = merge_runs(
            part_paths,
            self.run_dir,
            "output",
            self.fd,
            OUTPUT_KEY,
            self.config.max_records,
            self.config.max_open_files,
        )
        self._crash("stage3_after_merge")
        self.stats["merge_rounds"]["output"] = {
            "rounds": mstats.rounds,
            "per_round_input_runs": mstats.per_round_runs,
            "peak_input_runs": mstats.peak_input_runs,
            "peak_heap_bytes": mstats.peak_heap_bytes,
            "written_bytes": mstats.merge_written_bytes,
        }
        rows = 0
        out_bytes = 0
        if merged is not None:
            tmp_output = self.run_dir / "output.jsonl.partial"
            with self.fd.open(merged, "rb") as src, self.fd.open(tmp_output, "wb") as dst:
                for raw in src:
                    # 最终输出解包信封，只保留 out 负载；逐行流式写，不收集结果。
                    envelope = json.loads(raw.decode("utf-8"))
                    line = encode_line(envelope["out"])
                    dst.write(line)
                    rows += 1
                    out_bytes += len(line)
            tmp_output.replace(output_path)
        else:
            # 空结果（例如空侧 inner）也产出空文件并走同一提交路径。
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"")
        self._write_marker(STAGE3_MARKER, {"rows": rows, "bytes": out_bytes})
        self.stats["output_rows"] = rows
        self.stats["output_bytes"] = out_bytes

    # ------------------------------------------------------------------ 入口
    def run(self, output_path: Path, force: bool = False) -> dict[str, Any]:
        self.prepare_run_dir(force)
        stage1 = self._run_stage1()
        self._run_stage2(stage1)
        self._run_stage3(output_path)
        self.stats["fd_peak"] = self.fd.peak
        return self.stats
