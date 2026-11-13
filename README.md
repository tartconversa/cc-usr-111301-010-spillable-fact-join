# 倾斜键下有界内存事实联接执行器

本项目保存虚构算力设施的仓库源表、湖文件与 Python 工具。核心是一个不依赖
第三方库、严格受内存预算约束的外部联接执行器，用于在资产维度与计费明细
月末联接时稳定构建明细宽表，重点保证多重集语义与真实溢写，不含报表界面。
所有设备、组织和金额均为合成。

## 字段与单位

两侧 `row_id` 各自唯一；`key` 为 JSON 标量（字符串、整数、浮点、布尔）或
`null`；`value` 是合成整数金额。预算约束是**批记录数与序列化字节双约束**，
热点测试通过复制带独立 `row_id` 的输入构造。

`material-index.json` 记录全部输入内容哈希；`fixtures` 保存原始资料与配置，
计算输出放 `.workspace-output`，原始资料不被覆盖。

## 联接语义契约

- 支持 `inner` 与 `left` 两种联接；
- SQL 普通等号语义：`NULL` 不等于任何值，包括另一个 `NULL`，NULL 键不参与
  匹配；`left` 联接的未匹配行（含 NULL 键左行）右半部分输出为 `null`；
- **严格类型**：字符串 `"1"`、整数 `1`、布尔 `true`、浮点 `1.0` 是不同的键，
  类型标记决定全序 `null < bool < int < float < string`，绝不隐式合并；
- **多重集**：重复键输出完整笛卡尔乘积，不去重、不只保留一个维度行；
- 输出按 `(规范化键, 左 row_id, 右 row_id)` 全序排列；同键未匹配行（右为空）
  借存在性标记稳定排在真实匹配之前；
- 每行保留左右两侧完整原始身份。

## 算法：外排序归并 + 热点组磁盘分块交叉

选择外排序归并而非分区哈希的原因：输出要求全局有序，排序归并天然按目标全序
流式产出；哈希分区对热点键无能为力（热点永远落在同一分区），而排序后的同键
组可以切成受预算约束的磁盘块做嵌套循环交叉。

三个阶段，每阶段有独立完成标记，可独立恢复：

1. **阶段 1 双侧外排序**：流式读入，双约束缓冲触顶即排序落 run；单条序列化
   即超字节预算的记录进入专门的逐行 oversized 通道，不拆 JSON，计入
   `OversizedRecord` 例外；run 数超过单轮能力时做 FD 受限的多轮 k 路归并。
2. **阶段 2 排序归并联接**：按规范化键流式归组。普通组在内存交叉；超过预算
   的热点组两侧各切成磁盘 chunk 逐块交叉，即使单键组总量超预算也不要求
   “单键放得下”。超大行以单例块逐行参与。三个片段（正常×正常、超大左×正常、
   正常×超大右）各有私有 parts 清单与完成标记。
3. **阶段 3 输出归并提交**：全部片段按固定顺序汇总，多轮归并为全局有序流，
   逐行写最终输出，不先收集全部结果；仅在所有块完成且哈希校验通过后原子提交。

### 预算口径（可核验）

- 累积型缓冲（排序缓冲、热点组缓冲）同时受 `buffer_max_records` 与
  `buffer_max_serialized_bytes` 双硬约束，采用“加入前预判”，峰值最多等于
  预算；字节按 `normalization.encode_line` 的实际行编码统计，可与磁盘文件
  逐字节对照；
- 归并堆每路只持有一个首行，路数（fan_in）受记录预算与 FD 上限硬约束
  （`fan_in = min(max_open_files-1, max(1,max_records))`，减 1 留给输出句柄），
  其字节峰值经 `peak_heap_bytes` 如实报告。物理上无法在不放弃推进的前提下
  保证两个各自合规的首行之和也不超字节预算，故字节不对归并堆硬限；
- FD 经 `FDTracker` 包装引擎内所有 `open()`，峰值不超过 `max_open_files`；
- 物理不可行组合明确拒绝：`max_open_files<2`、或 `max_records=1` 且
  `max_open_files<3` 在入口报错；`max_open_files=2` 且确实溢写出多 run 时
  抛 `SpillConfigurationError`，而不是裸文件句柄崩溃。

统计覆盖：输入记录/字节/不同键数、各侧排序 run 数与峰值、各阶段归并轮次与
每轮输入 run 数、热点组数与 chunk 文件数、spill 记录数与字节、超大行例外、
输出行数与字节、FD 峰值。

### 恢复与提交

- run 目录指纹包含两侧输入的 SHA-256、字节数与执行参数（联接类型、两项预算、
  FD 上限）；输入字节或参数变化且未显式 `--force` 时拒绝复用旧 run；
- 所有 run/chunk/part 先写 `.partial` 再原子改名，崩溃后临时文件不会被当成
  完成产物；
- 片段完成标记只在该片段完整扫描结束后写入；中途崩溃的片段在恢复时整体重建
  （清理旧清单与孤儿块），因此**重启不会重复追加已完成笛卡尔块**；
- parts 清单逐块记录行数、字节数与 SHA-256，阶段 3 前逐项校验，输出仅在全部
  通过后提交。

## 金额多对多扩张（不平摊）

`src/amount_demo.py` 只做事实诊断：报告每个键的左右行数、笛卡尔行数、右表
原始金额与联接后被复制的金额及扩张倍数，并对多对多键给出明确警告。工具
**不自动平摊**——任何分摊都必须由业务规则显式指定，避免替业务做未授权的
会计假设。

## 运行方式

Python 3.11 以上，仅标准库。读取不受信任的外部 JSONL 时用隔离模式并显式
注入模块路径，避免工作目录被植入的 `json.py` 等模块劫持：

    PYTHONPATH 不被 -I 读取，统一用如下引导（脚本在仓库源码目录，输入在外部目录）：
    python -I -c "import sys; sys.path.insert(0,'$PWD'); \
        from src.external_join import main; raise SystemExit(main())" run \
        --left fixtures/left.jsonl --right fixtures/right.jsonl \
        --config fixtures/join.json --join-type inner \
        --run-dir .workspace-output/run-inner --output .workspace-output/inner.jsonl

常用参数：`--join-type {inner,left}`、`--max-records`、`--max-bytes`、
`--max-open-files`（覆盖配置预算）、`--fault {stage1_after_sort,
stage2_after_hot_chunk,stage2_after_part,stage3_after_merge}`（故障注入）、
`--force`（指纹变化时清理重建）、`--stats`（统计额外落盘）。

金额诊断（只读小输入）：

    python -I -c "import sys; sys.path.insert(0,'$PWD'); \
        from src.amount_demo import main; raise SystemExit(main())" \
        --left fixtures/left.jsonl --right fixtures/right.jsonl

退出码：0 成功；2 输入缺失；3 指纹不一致拒绝复用；4 故障注入模拟崩溃
（run 目录保留，重跑同一命令即可恢复）。

fixture 小例（左键 x 两行、NULL 一行；右键 x 三行）：inner 输出 6 行，
left 输出 7 行。

## 测试

    python -m compileall -q src tests
    python -I -c "import sys,unittest; sys.path.insert(0,'$PWD'); \
        raise SystemExit(unittest.main(module=None, argv=['x','discover','-s','tests','-v']))"

测试包含：联接语义与严格类型、对照朴素内存参考的固定种子随机差分、极小预算
强制多轮 spill 与多轮归并、单热键磁盘分块、空侧、超大行、FD 上限、故障注入
与跨进程恢复、指纹复用拒绝、CLI 端到端。

## 固定容器

    docker build -t spillable-fact-join:local .
    docker run --rm spillable-fact-join:local

容器镜像以完整 digest 固定，默认运行测试套件，无第三方依赖。
