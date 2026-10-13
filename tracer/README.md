# 行事件与局部变量快照诊断

这是 `taco-mutation-light` 的独立后处理组件。读取 SPEC、两份源码、固定输入及执行合同，在隔离容器中分别进行普通执行和行级追踪，保存原始轨迹、脚本比较及可选的 DeepSeek 分析。不会生成变异体或新测试，不修改已有样本、判题结果和盲标表。

交付时仅进行了源码静态检查；没有运行本组件、测试、容器或 API，也没有读取 `.env`。下面的命令供检查后由用户执行。真实采集性能、Docker 故障恢复及实际模型答复尚未验证，不能把实现或语法检查当成验收通过。

## 输入与入口

所有命令在 **WSL Ubuntu** 的项目根目录执行，使用已有解释器，无需新增第三方 Python 依赖：

```bash
cd /home/llynn/forICML/taco-mutation-light
```

默认从 `config.toml` 中 `data.run_dir/state.sqlite` 的 `samples` 表只读取得已保存样本。也可以提供 `--database /绝对路径/state.sqlite`，或通过 `--samples` 指定一个 JSON 对象、JSON 数组、JSONL（兼容已有 `materials.jsonl`）。一条输入对应一个原程序×变异体×固定输入。

必填字段为 `spec`、`original_code`、`mutant_code`、`execution_contract`、`input`。light 样本的 `witness.input` 自动复用。`expected` 可选，存在不表示已经验证正确；没有 expected 也可对比行为，原程序不作为正确答案 oracle。`execution_contract.entry` 复用现有明确入口配置：普通函数、Solution、stdin/stdout、参数排序、指定可变参数输出、树/链表表示。函数输入为完整位置参数数组，stdin 输入为完整字符串，不根据执行结果猜转换。

参考 `examples/pair.json`，其中是合成文档示例，不是正式 TACO 数据。自建输入可使用相同格式；输入合法性未经独立确认会作为分析不确定性保留。

## 检查、采集、分析与恢复

只读预览，不接触 Docker、凭据或网络，不创建输出目录：

```bash
.venv/bin/python -B -m tracer inspect
.venv/bin/python -B -m tracer inspect --samples tracer/examples/pair.json
```

只采集、比较，不请求 LLM；下面会实际执行题目，应在检查后再运行：

```bash
.venv/bin/python -B -m tracer run
```

启用 DeepSeek 分析（会实际执行缺失的采集步骤并发送付费请求）：

```bash
.venv/bin/python -B -m tracer run --paid --max-requests 60
```

若此前已完成采集，此命令复用已提交的轨迹与比较结果，只处理尚未分析的范围。再次运行同一命令为恢复；同输出目录已有的完整回答、失败及未知请求均不自动重发。`--max-requests` 是该诊断输出目录累计尝试上限，包含旧尝试、失败和未知，不是每次新增额度。单个输入范围最多一次分析尝试。不要通过更换输出目录“恢复”未知请求。

小规模检查可使用 `--limit 1`，以后去掉该参数处理剩余材料。显式输入示例：

```bash
.venv/bin/python -B -m tracer run --samples tracer/examples/pair.json --output runs/tracer-example
.venv/bin/python -B -m tracer run --samples tracer/examples/pair.json --output runs/tracer-example --paid --max-requests 1
```

状态查询及遗留容器清理：

```bash
.venv/bin/python -B -m tracer status
.venv/bin/python -B -m tracer cleanup
```

若之前使用自定义 `--output`，这些命令也须传入相同目录。cleanup 联系 Docker，但不执行题目、不读凭据、不发模型请求；只处理该目录登记的容器，不做全局 prune。若发现同项目其他输出目录的遗留容器，暂停创建并要求先对原目录执行 cleanup。

## 模型与费用

模型固定 `deepseek-v4-pro`，复用根目录 `config.toml` 的 DeepSeek Chat Completions 通信配置、thinking/reasoning_effort 和超时；诊断默认最大输出 4096 tokens。凭据路径固定为项目根目录 `taco-mutation-light/.env`，仅在 `run --paid` 的实际请求边界由现有 `llm.py` 读取，保持其用户所有、0400/0600和环境冲突检查。不复制凭据，不把密钥传入容器。

默认一次对比一次调用，不使用模型工具循环或自动修复请求。材料默认最多 100000 字节，优先减少事件摘录，仍放不下完整源码/SPEC则记录 `material_too_large`，不付费调用。它是字节限制，不是精确输入 token 上限。记录返回用量，费用保持 `unknown`；本版提供累计请求数和输出 token 限制，**没有供应商计费保证或硬美元上限**。`--paid` 前应据选定样本数检查这一范围。

## 输出

默认目录 `runs/tracer/`，支持 `--output`：

```text
state.sqlite                         独立任务/请求/容器状态
summary.json                         本次运行完成后的汇总
traces/<trace_key>/
  baseline.json                      普通执行观察
  <attempt>.jsonl.gz                 按顺序保存的真实行/调用/返回/异常事件
  metadata.json                      轨迹摘要、普通/诊断一致性、环境与完整性
pairs/<pair_id>/
  input.json                         本次固定输入材料
  comparison.json                    对齐、候选首次分歧、差异序列与事件摘录
  result.json                        阶段结果及工件引用
analysis/<pair_id>/
  request.json                       实际请求材料与配置，不含认证头
  response.json                      原始结构化答复与用量（非逐字HTTP报文）
  analysis.json                      结构/引用校验后的分析及待人工审核状态
  failure.json                       脱敏失败类别，若发生
```

未提交完成标记的轨迹文件保留为中断工件，不当作完整缓存。有效轨迹按源码、输入、合同、镜像、追踪代码及配置绑定；相同原程序输入并发请求会按键去重。更换模型、采集限额或运行代码时要求新输出目录，避免混用版本；并发数量可以在原目录调整。原始轨迹不进入默认盲标导出。

单条事件示例（示意，非运行证据）：

```json
{"kind":"event","event_id":3,"event":"line","phase":"before","line":3,"function":"solve","call_id":"root/solve#1","locals":{"x":{"type":"int","value":3},"y":{"type":"int","value":4}},"object_id_scope":"this_snapshot_only","value_gaps":0}
```

行事件发生在执行该行之前。同一行每次执行均保留独立事件，不采样跳过循环。值在回调现场冻结；普通输出由原适配器捕获，轨迹在独立的带类型传输帧中流式输出、压缩落盘。

局部变量的精确内置标量、容器和安全的普通实例字典可采集；不调用用户 repr、属性、deepcopy或序列化方法。对象编号仅在**单条快照内**有效，保存别名/循环引用；不声称跨快照对象生命周期身份。槽位对象、自定义元类等不支持对象明确记缺失。容器、字符串、嵌套及总事件均有限额，不能把截断轨迹称为完整轨迹。

## 并发、资源和恢复

`tracer/config.toml` 默认六个采集工作槽、最多两个同时启动容器、两个比较进程、两个 LLM 线程、最多十二个在途样本。样本以动态队列调度，与算子家族无关。任一样本采集完即可比较、比较完即可分析，不等待整批。线程等待 API 时不占容器槽。比较进程只接收文件路径和公开材料，不共享 SQLite 连接。

第一次处理一组完全未缓存的材料会运行两侧各一次普通执行、一次诊断执行，合计四次目标执行；每个采集工作槽按顺序执行这些步骤，不默认翻倍成十二个同时运行的容器。相同原程序输入可复用完整记录。普通/诊断均使用诊断配置下的资源限制，不改写原管线判题。

容器保持每次独立创建、禁网、非特权、只读源码、独立 tmpfs，复用现有隔离配置核验并增加 Python 3.11.9核查。默认 512MiB/1CPU（继承根配置），诊断墙钟30秒、启动45秒、20000事件、轨迹32MiB；所有数字为初始配置，尚未压测。

为避免两个独立 runner 各启动六个容器，本版 tracer 运行期间持有原 light run 锁及项目级 tracer 锁：**不与原生成管线或另一 tracer 调度进程同时运行**。这是实现中明确采用的简单总量保护；tracer 内部六路和分阶段并发不受影响。将来如需要生成/追踪同时运行，再共用资源调度器。

容器在 create 前持久登记，确认删除才移出登记。清理失败不覆盖执行结果，但阻止新建。Docker 暂不可用暂停采集，已落盘的比较和 LLM继续；一个维护线程按延长间隔重新核验隔离条件，恢复后续跑。它不自动重启 Docker。磁盘空间不足同样暂停采集。Ctrl+C停止派发并等待在途工作收尾；模型调用可能需等待其配置的总超时。

## 解释边界

- 只追踪被测 `source.py` 主线程。生成器/协程帧不采用普通调用模型，明确记录缺口；第三方库及原生扩展内部不采集。
- 函数快照只有局部变量；函数使用的全局状态可能未覆盖。返回和异常另有事件。
- 对齐使用相同源码行、调用路径、重复次数及顺序，是启发式对应；改动行和无法对应的事件不强行匹配。
- `first_divergence` 最多为成功比较快照中的最早候选差异，不是完整执行的首次差异证明。
- `propagation.edges=[]`。只保存已观察差异序列，模型解释为 inferred/unknown；本版不采集动态读写依赖。
- 可变对象比较忽略快照局部编号；别名关系、循环内容、集合无序相等性不作可靠差异断言。缺值不等于相等。
- 每侧诊断一次，检查普通/诊断外部结果一致性；没有声称已验证重复轨迹稳定性，也不声称插桩/追踪对任意Python程序透明。
- SPEC和程序文本作为模型数据，不作为指令。校验模型格式和事件引用，不等于证明其解释正确。
- 收集器与目标程序同处一个解释器，不是防恶意自省或篡改追踪的证明系统；Docker提供运行隔离。

## 用户检查后的验证命令

纯离线单元测试，使用模拟采集/模型，不运行目标源码、Docker或HTTP：

```bash
.venv/bin/python -B -m unittest tracer.test_tracer -v
```

独立的真实容器测试（会执行合成程序，不读密钥、不发API，保留 `runs/tracer-selftest-*` 证据）：

```bash
.venv/bin/python -B -m unittest tracer.test_docker -v
```

它检查隔离、普通/追踪结果一致性及六个实际目标执行区间重叠，不能替代高负载快照压测或真实 Docker 崩溃恢复验证。尚待用户批准运行的重点包括：大容器/长循环的资源峰值、Docker中断与清理失败、进程半写中断恢复，以及真实 DeepSeek 输出兼容性。
