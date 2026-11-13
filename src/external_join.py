"""命令行入口：运行有界内存外部联接并输出可核验的执行统计。

示例：
    python -m src.external_join run \\
        --left fixtures/left.jsonl --right fixtures/right.jsonl \\
        --config fixtures/join.json --join-type inner \\
        --run-dir .workspace-output/run --output .workspace-output/inner.jsonl

配置文件中的预算项可被命令行覆盖；统计以 JSON 打印到 stdout，日志到 stderr。
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from .engine import EngineConfig, ExternalJoinEngine, InjectedCrash, RunReuseError

FAULT_POINTS = (
    "stage1_after_sort",
    "stage2_after_hot_chunk",
    "stage2_after_part",
    "stage3_after_merge",
)


def load_config(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("配置文件必须是 JSON 对象")
    return data


def build_config(args: argparse.Namespace) -> EngineConfig:
    raw = load_config(args.config)
    max_records = args.max_records if args.max_records is not None else raw["buffer_max_records"]
    max_bytes = args.max_bytes if args.max_bytes is not None else raw["buffer_max_serialized_bytes"]
    max_open_files = (
        args.max_open_files if args.max_open_files is not None else raw["max_open_files"]
    )
    if args.join_type not in tuple(raw.get("join_types", ("inner", "left"))):
        raise ValueError(f"配置未声明的联接类型: {args.join_type}")
    if args.fault is not None and args.fault not in FAULT_POINTS:
        raise ValueError(f"未知故障注入点: {args.fault}，可选 {FAULT_POINTS}")
    return EngineConfig(
        join_type=args.join_type,
        max_records=int(max_records),
        max_bytes=int(max_bytes),
        max_open_files=int(max_open_files),
        fault=args.fault,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="有界内存外部联接执行器（inner/left，多重集语义）")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="执行联接（支持断点恢复）")
    run_p.add_argument("--left", required=True, type=Path)
    run_p.add_argument("--right", required=True, type=Path)
    run_p.add_argument("--config", required=True, type=Path)
    run_p.add_argument("--join-type", required=True, choices=("inner", "left"))
    run_p.add_argument("--run-dir", required=True, type=Path)
    run_p.add_argument("--output", required=True, type=Path)
    run_p.add_argument("--max-records", type=int, default=None, help="覆盖配置的记录批次上限")
    run_p.add_argument("--max-bytes", type=int, default=None, help="覆盖配置的字节批次上限")
    run_p.add_argument("--max-open-files", type=int, default=None, help="覆盖配置的 FD 上限")
    run_p.add_argument("--fault", choices=FAULT_POINTS, default=None, help="测试用故障注入点")
    run_p.add_argument("--force", action="store_true", help="指纹变化时强制清理旧 run 重建")
    run_p.add_argument("--stats", type=Path, default=None, help="统计 JSON 额外写入路径")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command != "run":  # pragma: no cover - argparse 已保证
        parser.error("未知子命令")

    for label, path in (("left", args.left), ("right", args.right), ("config", args.config)):
        if not path.exists():
            print(f"错误：{label} 输入不存在: {path}", file=sys.stderr)
            return 2

    config = build_config(args)
    engine = ExternalJoinEngine(args.left, args.right, config, args.run_dir)
    try:
        stats = engine.run(args.output, force=args.force)
    except RunReuseError as exc:
        print(f"错误：拒绝复用旧 run — {exc}", file=sys.stderr)
        return 3
    except InjectedCrash as exc:
        print(f"模拟崩溃：{exc}（run 目录保留，重新执行同一命令即可恢复）", file=sys.stderr)
        return 4

    stats["config"] = asdict(config)
    stats["algorithm"] = (
        "external_sort_merge_join：双侧外排序归并，热点组磁盘分块交叉，"
        "FD 受限时多轮归并，输出流式提交"
    )
    payload = json.dumps(stats, ensure_ascii=False, indent=2)
    print(payload)
    if args.stats is not None:
        args.stats.parent.mkdir(parents=True, exist_ok=True)
        args.stats.write_text(payload + "\n", encoding="utf-8")
    print(f"输出已提交: {args.output}（{stats['output_rows']} 行）", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
