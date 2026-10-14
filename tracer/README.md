# 行事件与局部变量快照诊断 v2

这是 `taco-mutation-light` 的独立后处理组件：输入 SPEC、原程序、变异体、固定输入及执行合同，输出逐行执行证据、机械比较和可选的 DeepSeek 候选解释。原变异体生成、官方测试判定、样本及盲标导出流程保持原样。

本次只做静态检查、离线单元测试和已有轨迹重比较；**没有实际采集、运行题目、启动 Docker、调用 LLM API 或读取 `.env`**。验证记录见 [VALIDATION.md](VALIDATION.md)。下面的采集、容器测试和付费指令供用户检查后执行。

## 六部分改动

| 部分 | 实现和边界 |
| --- | --- |
| 运行时噪声与覆盖率 | `adapters/policy.py` 区分未显式使用的解释器变量、符号摘要和实际未采集值；事件、变量值、检查点、对齐分别报告完整性。 |
| 大数组适配 | `adapters/sequences.py` 保存长度、首尾及整数局部变量附近的索引窗口；`adapters/checkpoints.py` 流式保存首次可见和作用域退出时的大序列检查点。 |
| 机械执行证据 | `compare.py` 对齐源码锚点、兼容改动行和调用边界，有限窗口重新对齐；比较已采到的值路径，保留未匹配事件。全部对应关系标为启发式。 |
| LLM 候选解释 | `analysis.py` 一次调用生成 first_divergence、propagation、trigger_condition、requirement 和 SPEC 引文；引用与格式校验不等于语义验证。 |
| 字段职责 | `facts.py` 机械组装 observable_consequence 和 oracle 的 target/kind/comparison，LLM 只补 requirement/SPEC；不生成 behavior_description 或行为标签。 |
| 阶段、恢复与资源 | collect/compare/analyze 可独立执行，run 可重叠调度；保留六路动态采集、容器登记清理、持久阶段和请求限额，增加版本及资源指标。 |

## 输入和分阶段执行

在 WSL Ubuntu 的项目根目录执行，不需要新增 Python 第三方依赖：

```bash
cd /home/llynn/forICML/taco-mutation-light
```

默认只读根目录 `config.toml` 中 `data.run_dir/state.sqlite` 的 samples 表。`inspect`、`collect`、`run` 也接受 `--database /path/state.sqlite` 或 `--samples /path/pairs.jsonl`；支持 JSON 对象/数组、JSONL 和已有 materials.jsonl。一条材料是一组原程序 × 变异体 × 固定输入。

必填 `spec`、`original_code`、`mutant_code`、`execution_contract`、`input`；已有 light 样本自动复用 `witness.input`。`expected` 可选，存在不表示已验证正确。原程序输出不作为正确答案依据。合同复用已有入口、参数排列、Solution、stdin/stdout、指定可变参数输出、树/链表转换。函数输入为完整位置参数数组，stdin 为完整字符串；不依据执行结果猜转换。合成格式示例见 `examples/pair.json`。

以下是分阶段指令，**此次没有执行采集和分析指令**：

```bash
# 1. 只读检查，不读密钥、不执行题目
.venv/bin/python -B -m tracer inspect

# 2. 实际采集；先取一对检查，确认后去掉 --limit 1 处理余下材料
.venv/bin/python -B -m tracer collect --output runs/tracer-v2 --limit 1

# 3. 只使用已保存轨迹比较，不接触 Docker 或 API
.venv/bin/python -B -m tracer compare --output runs/tracer-v2

# 4. 对已比较的材料调用 DeepSeek；1 是该目录累计请求上限
.venv/bin/python -B -m tracer analyze --output runs/tracer-v2 --paid --max-requests 1

# 查看阶段、请求及容器状态
.venv/bin/python -B -m tracer status --output runs/tracer-v2
```

扩展到其余样本时，重复 collect（去掉 limit）和 compare，再按批准的累计请求上限执行 analyze。并发池采用动态任务队列，与算子家族无关。独立阶段可以分别重跑；同一输出目录同时只允许一个调度进程。

若希望任一样本采集完成就立即比较，而不等待整批，使用 `run`：

```bash
# 连续采集、比较；不调用模型
.venv/bin/python -B -m tracer run --output runs/tracer-v2

# 连续采集、比较、付费分析；60 是示例累计上限，需用户确认后使用
.venv/bin/python -B -m tracer run --output runs/tracer-v2 --paid --max-requests 60
```

只重比较旧的 `runs/tracer` 证据，无需重新执行程序：

```bash
.venv/bin/python -B -m tracer compare --source runs/tracer --output runs/tracer-recompare-v2
.venv/bin/python -B -m tracer status --output runs/tracer-recompare-v2
# 后续若确认需要分析，显式启用；这个命令不会补采集
.venv/bin/python -B -m tracer analyze --output runs/tracer-recompare-v2 --paid --max-requests 60
```

`compare/analyze` 只接受冻结的 `--source` 工件（省略时为 output），不接受新的 samples/database。新目录引用旧轨迹文件而不复制大文件，因此旧目录必须保留；导入会校验轨迹、检查点、比较文件哈希。旧请求记录全部导入，包括未被 limit 选中的请求。请求响应工件复制到新目录，源目录不被继续分析改写。

## 采集粒度与大对象策略

仍采用 `sys.settrace`，不改写被测源码。采集 source.py 主线程的 call、line、return、exception，以及异常退出标记。**line 快照在该行执行之前**；该行效果通常在下一事件或 return 中出现。不是每一条表达式、对象读写或寄存器操作的日志。

变量值必须在采集回调现场冻结，避免对象后续变化污染历史快照，因此 values 编码仍属于采集步骤。文件比较和 LLM 分析使用独立工作池，可以和其他样本的采集重叠。

小对象完整记录到嵌套、节点、字符串及快照预算为止。精确内置 list/tuple 长度超过 128 时，普通快照记录：

- 类型、长度；前 8 项、后 8 项。
- 最多 8 个整数局部变量附近、半径 2 的索引窗口；索引去重并保留实际下标。
- omitted_count、coverage=partial；这些整数只是窗口提示，**不代表实际访问过该索引**。

例如下列片段是格式示意，不是实际执行结果：

```json
{"D":{"type":"list","length":100001,"coverage":"partial","selection":"head_tail_local_integer_windows","sampled_items":[{"index":99348,"value":{"type":"int","value":17}}]}}
```

完整序列检查点独立写 gzip JSONL：start 元数据、带 offset 的 items 分块、end 完整性结尾。每个调用实例内，以局部变量名作为保存单位：

- 变量首次可见时如果已是大 list/tuple，保存 first_visible 检查点；开始时很小的对象由普通行快照保存。
- 曾成为大序列、退出时仍可见的变量保存 scope_exit 检查点，即使已缩小。
- 大返回值也保存，和同次退出已保存的同一对象去重。
- 循环中反复赋值不会对每次重绑定生成一份全量检查点；这不是对象生命周期追踪。
- 删除、重绑定成非序列、作用域/变量/检查点额度不足均显式记缺口。推导式临时作用域不生成全量检查点，避免在循环中反复复制临时大列表。

完整检查点目前针对**直接局部变量或返回值上的精确内置 list/tuple**。大字典、集合、嵌套对象图、NumPy/自定义容器没有新增全量适配。检查点逐项编码，嵌套项仍受深度、节点和容器项数限制；循环/跨项别名不是完整对象图。超时或进程被杀时可能没有退出检查点。`complete=true` 表示此次检查点支持范围内的值已记录完，不表示整条程序执行链完全覆盖。

不调用用户 repr、属性、deepcopy 或序列化方法。普通实例只读取安全内置 __dict__ 描述符；槽位、自定义元类等明确缺失。对象编号只在单条快照内有效。函数/模块/类为符号摘要，不把其实现或内部状态当成已采集。未被源码显式使用的解释器注入名称会排除；显式使用这些名字或使用 globals/locals/vars/eval/exec 时保守保留。这是静态分类规则，不能识别所有间接动态访问。

## 默认预算与性能边界

| 项目 | 默认上限 |
| --- | --- |
| 活跃采集 / 同时启动 / 未移除容器 | 6 / 2 / 8 |
| 比较进程 / LLM 线程 / 在途样本 | 2 / 2 / 12 |
| 诊断墙钟 / 容器启动 | 30 秒 / 45 秒 |
| 行事件 / 事件流字节 | 20000 / 32 MiB |
| 单行帧 / 单快照 / 全程变量值预算 | 128 KiB / 64 KiB / 24 MiB |
| 单检查点 / 全部检查点 | 256 MiB / 512 MiB（每次被测执行，压缩前） |
| 检查点数量 / 单个元素数 / 单个时限 | 64 / 10^7 / 5 秒 |
| 检查点分块 / 活跃作用域 / 每作用域变量 | 32 KiB / 128 / 128 |
| 每对检查点机械比较元素总量 | 10^6 |
| 启动前最低空闲磁盘 | 4 GiB |
| 模型材料 / 事件摘录 / 输出 token | 100000 字节 / 40 条 / 4096 |

`10^7` 只是元素数上限，**并不承诺能完整保存 10^7 元素**：类型化 JSON 字节、单项大小、5 秒时限、总墙钟和磁盘上限可能先达到。序列检查点线性读取；普通大数组快照只读取选定下标，但每行仍需遍历局部变量名。检查点比较独立限额，即使全量文件已保存，也不会无界扫描全部内容。全量数组不发送给模型，只发送比较摘要和选出的事件。

逐行变量值预算耗尽后保留事件骨架；事件数/总事件流预算耗尽后停止行事件，但追踪回调继续尝试作用域退出检查点。停止原因分别保存，采到了结果不等于采到了完整中间过程。检查点与原始事件分别 gzip 流式落盘，避免在宿主机或容器里复制整份编码大数组。

完全无缓存时，一对材料执行两侧各一次普通运行和一次追踪，共四次；相同原程序、输入和合同可复用。每个采集槽顺序运行这些步骤，不默认变成 12 个活跃容器。容器内存/CPU继承根执行配置，当前为 512 MiB / 1 CPU。六路在资源配置层面的内存上限约 3 GiB，另有宿主比较进程、Docker 和系统开销；不是实际 RSS 测量。真实采集耗时、磁盘压缩比和 API 费用此次尚未实测。

summary.json 记录阶段耗时累计、去重轨迹数、事件/检查点压缩前后字节、不完整轨迹、未匹配事件、普通/追踪执行耗时、host-ready 到 result 区间峰值。区间峰值含被复用的历史执行区间，不是 CPU 并行度或本次实时容器监控。

## 机械证据和模型解释

比较器尝试同源码行、同类 AST 语句的等长替换、函数入口/出口和有限窗口重新对齐。因此改动 return 行不再导致其返回值整体漏比较，循环次数不同也可以保留仍可对齐的后续事件。函数调用次数不同，须有双方唯一、完整的入口值签名才尝试对应，否则保留未匹配；同次数调用的序号对应仍是启发式。检查点只继承实际事件对齐得到的调用对应。

未知值或大对象缺失部分不作相等断言；已知长度及双方都记录的下标/字段可以比较。不强制所有变量或函数改名对应，不建立动态依赖图。

| 字段 | 来源与含义 |
| --- | --- |
| comparison.first_divergence | 已比较快照中的最早差异候选；非全局首次差异证明；原轨迹不完整时保持 unknown。 |
| comparison.propagation | observed_difference_sequence 是观察序列；edges 留空，状态 unknown。 |
| analysis.first_divergence / propagation | DeepSeek 基于提供证据生成的 inferred/unknown 候选解释，有引用、局限，待人工核验。 |
| analysis.trigger_condition | 限当前固定输入的候选触发解释，不证明必要充分条件；证据不足可 unknown。 |
| observable_consequence | 机械比较已记录的输出渠道、异常、完成状态和 stdout；不判哪一侧正确，也不声称覆盖任意交互。 |
| oracle_type | target/kind/comparison 来自原 execution_contract；例如 return / exact_value / json_exact。是“检查什么、按什么规则检查”的结构，不是行为类别标签。 |
| requirement / spec_basis | 同一次模型请求补充待审核的要求及逐字 SPEC 引文。引文必须能在 SPEC 中找到，才能通过结构校验。 |

模型不会填 behavior_description、行为标签或盲标表。相关人工总结、行为标签及独立审核流程仍由后续研究流程负责。模型候选材料含源码、diff 可推导信息及运行证据，不能直接当作盲标材料。

模型材料保留两侧事件名额，优先最早候选差异附近、最终事件、后续差异和未匹配邻域。超预算逐步缩减两侧摘录；连最小双侧摘录连同完整源码/SPEC仍放不下时，记录 material_too_large_for_balanced_evidence，不发送请求。每侧实际发送数明确记录；模型没有看到全轨迹。

## 模型、版本与恢复

模型仍为 `deepseek-v4-pro`，复用根 config.toml 中供应商、端点、thinking/reasoning_effort、超时及现有 llm.Client。凭据仅在显式 `analyze/run --paid` 的真实请求边界读取项目根 `.env`，保持其权限和环境冲突检查；不进入容器或工件。

每个语义材料范围最多一次请求；无自动模型修复或重试。`--max-requests` 是输出目录累计尝试上限，包含导入的历史、失败和 unknown，不是新增额度。更换比较版本、模型参数或输出目录导入旧材料后，都不会自动重发旧请求；会显示 existing_request_for_other_version 或保留原状态。保留响应但中断本地校验的请求可以只重做本地校验。

记录供应商返回用量，费用为 unknown。本组件仍只有请求数和输出 token 限制，**没有硬美元预算**；不继承 motherdata 的付费授权/预算账本。没有新增付费调用。

采集缓存绑定源码、固定输入、合同、镜像、追踪代码及采集限额。改变采集策略需新输出目录；只是改变比较器/分析设置可重用旧轨迹，比较/分析工件按版本保存。旧轨迹缺失的信息不能靠重比较补出来。新默认输出为 runs/tracer-v2，旧 runs/tracer 保留。

```text
state.sqlite                                独立阶段、请求、容器登记
summary.json                                状态和资源指标
traces/<trace_key>/
  baseline.json                             普通执行观察
  <attempt>.jsonl.gz                        行/调用/返回/异常事件
  <attempt>.jsonl.gz.checkpoints/cp*.jsonl.gz 大序列分块检查点
  metadata.json                             文件哈希、检查点索引、观察一致性及覆盖情况
pairs/<pair_id>/
  input.json                                固定材料
  comparisons/<version>/comparison.json      机械证据和双侧摘录
  result.json                               当前阶段和工件引用
analysis/<pair_id>/<version>/
  request.json / response.json               请求材料、结构化答复和用量
  analysis.json                             候选解释及待人工审核状态
  failure.json                              脱敏失败记录（若发生）
```

Docker 仍采用单次容器、禁网、非特权、只读源码、独立临时目录，并核验 Python 3.11.9。创建前登记，确认移除后释放登记；清理失败阻止继续创建。Docker 短暂不可用只暂停新采集，已落盘的比较和分析可以继续；健康检查退避后恢复，不自动重启 Docker。磁盘不足也暂停。Ctrl+C停止派发并等待在途工作，不重发未知请求。

collect/run/cleanup 持有原 light run 锁及项目 tracer 锁，防止原生成管线或另一个 tracer 同时创建容器超额；compare/analyze 只锁诊断目录，独立阶段不启动 Docker。清理只针对相同输出目录登记的容器，不做全局 prune：

```bash
.venv/bin/python -B -m tracer cleanup --output runs/tracer-v2
```

## 覆盖边界与验证命令

源文件主线程之外、第三方库/native 内部、生成器/协程、独立的全局变量快照、实际读写依赖、跨快照对象身份、任意外部交互尚未采集。解释器行事件也不能表示同一行每个子表达式。异常退出识别与运行时版本绑定。追踪与被测程序同处一个解释器，不能作为防恶意篡改的证明系统。普通/追踪输出一致只说明本次记录一致，不证明追踪透明或多次运行稳定。

纯离线测试（已执行；模拟帧、文件和模型响应，不执行被测源码）：

```bash
.venv/bin/python -B -m unittest tracer.test_tracer tracer.test_v2 -v
```

用户检查后可以执行以下真实隔离测试。**此次未运行**，会执行合成程序、验证六路真实区间重叠及大数组检查点/索引窗口，保留 runs/tracer-selftest-*；不调用 API：

```bash
.venv/bin/python -B -m unittest tracer.test_docker -v
```

之后再按前面的 collect → compare → analyze 分阶段检查真实材料。真实 Docker 中断、容器清理故障、高负载数据、10^7 数组、实际 DeepSeek 输出兼容性仍需要单独验证。
