# taco-mutation-light

从固定 100 个 TACO 候选任务出发，LLM 只生成变异体，用已有官方案例找见证，导出每家族 10 条、合计 60 条试标材料。不生成新测试，不从题面提取输入，不预填人工行为结论。

## 配置与运行

在 WSL Ubuntu 中运行，Python 3.11+；Docker Desktop 的 Ubuntu 集成需开启。目标源码只在禁网络、非特权、只读源码、独立 tmpfs 的 Docker 容器中执行。Docker 不可用时停止，没有宿主机回退。

```bash
cd /home/llynn/forICML/taco-mutation-light
# 当前目录已准备好 .venv；以下两条用于在其他机器重新安装。
uv venv --python python3 .venv
uv pip install --python .venv/bin/python pyarrow

.venv/bin/python -B pipeline.py prepare
.venv/bin/python -B pipeline.py check
.venv/bin/python -B pipeline.py status
```

`prepare` 只读题并保存候选池；`check` 只运行容器隔离探针；两者不读取 API 凭据、不发送 API 请求。JSON/JSONL 输入不需要 pyarrow。镜像默认 `python:3.11.9-slim`，本机已存在；新机器请自行 `docker pull python:3.11.9-slim`，管线不隐式下载镜像。

编辑 `config.toml` 中的 `api.credential_file`，填写明确的本地 `.env` 文件绝对路径。文件属于当前用户，权限为 0600 或 0400，内容为 `DEEPSEEK_API_KEY=...`。不搜索凭据、不执行 dotenv 内容、不记录密钥；只在带 `--paid` 的生成请求中读取。若环境中已有不同的同名密钥，报错而不覆盖。

```bash
# 这条命令会调用付费 API，也会执行官方案例。
.venv/bin/python -B pipeline.py run --paid

# 恢复：再次运行同一命令即可。
.venv/bin/python -B pipeline.py run --paid
.venv/bin/python -B pipeline.py status
.venv/bin/python -B pipeline.py export
```

没有脚本内总请求数或美元额度门禁；记录每次请求的模型、实际提示、用量、结束原因和错误。费用不估算，缺失用量保留未知。连接、读取空闲、总等待超时在 config 中分别可改；超时不是全局预算。

## 生成、并发与结束

- 六个家族各一个线程，网络请求与目标执行均可真实重叠；默认 `execution.workers=6`。原程序核验按题共享，网络和执行期间不持有全局锁。某家族满额后该线程结束，后程并发自然减少。
- 100 个任务按输入文件的稳定顺序去重选取，再按 seed 打乱。缺输入和格式不支持也占候选池位置，不自动补第 101 个；不是对整个 TACO 的均匀随机抽样。
- 每题依原顺序尝试前 `data.max_solutions` 个解，默认 3；选择第一个通过全部可用官方案例的受支持解。全部失败则跳题，静态检查不会执行源码。
- `strategy="per_family"` 对各适用家族分配机会；`per_task_random` 按 seed 为每题选 `families_per_task` 个适用家族。`opportunities` 是每个选中题目×家族的生成机会数。一个响应一个候选，取得见证后该题该家族早停。
- 默认每对生成一次。传输失败、响应截断、JSON/补丁/形式错误记录后，先继续其他任务，首轮结束时补跑一次。两次均失败就保留失败，不自动换模型。家族已经满额时不为清空失败队列继续请求。
- `skip`、重复变异体、正常生成但官方测试未见差异，不属于技术失败。后续机会携带此前成功重建的简短补丁，始终相对原程序生成。
- 默认遇到第一个稳定见证即停止该变异体测试；`repeat_witness=true` 对见证原程序和变异体各复验一次。`all_official_tests=true` 可继续执行全部可用案例。未知执行与不稳定观察分别记录，不算“无差异”。
- 每条配额按不同变异体计，附一个主要见证；同一变异体不会用多个输入凑数。同题可贡献不同家族，报告另列独立题数。
- 每族 10 条、合计 60 条时停止新派发；100 题及必要补跑结束后，45–59 条为可采纳试标批次，30–44 条提示检查损耗，不足 30 条建议修改管线。这些阈值都在 config 中。

`Ctrl+C` 停止安排后续工作，并等待正在进行的工作收尾。进程被强制结束时，恢复将未完成请求记为 unknown，按剩余一次补跑机会创建新请求；不改写旧请求结果或伪造连接关闭。执行结果、已收到的变异响应及次数保存在 SQLite 中，重启不会清零。

同一个 run 只允许一个调度进程，内部六个线程共享它。修改模型、提示和策略影响后续工作，已完成记录不重算，旧失败不因换模型重获次数。如需新候选池或独立实验，修改 `data.run_dir`；如有意追加生成机会，增加 `opportunities`。隔离环境和输入变化会自然使用新的执行缓存，不需要重签协议锁。

## 输入和适配

`data.source` 支持 TACO Parquet 目录/单文件、JSON 数组、JSONL，或旧项目包含 `definition` 的适配材料。原始字段是 `question`、`solutions` 和 `input_output`（其中 `fn_name`、`inputs`、`outputs`）；也可直接提供 `spec`、`solutions`、`official_inputs`、`official_outputs`、`contract`。

复用的能力：stdin/stdout（包括 `stdin.buffer`）、普通函数、无参构造的 Solution 方法、参数排列、return/stdout/指定可变参数输出、明确字段映射的树和链表。输入/输出支持显式 `as_stored`、`single_wrapped`、`lines_to_text`、`text_to_number`、`number_to_text` 转换。不会在执行失败后轮番尝试转换直到答案“碰巧正确”。

`data.overrides` 可指向一个 JSON 对象，以 task_id 为键。例如：

```json
{
  "任务的 task_id 或 URL": {
    "representation": {"output": "single_wrapped"},
    "execution_contract": {
      "entry": {"mode": "function", "fn_name": "solve", "parameter_order": [0]},
      "comparison": {"kind": "json_exact"},
      "exception_policy": "forbidden"
    }
  }
}
```

树/链表配置沿用旧适配器：`entry.structures` 按输入参数索引指定 `kind`（binary_tree/linked_list）、`encoding`（level_order/values）、`fields`（value/left/right 或 value/next），可选 class_name；`output_structure` 同格式。声明 `output_channel="mutated_argument"` 时用 `output_argument` 选择调用参数。

文本比较支持 text_exact/text_lines/text_tokens；函数默认 json_exact（区分 bool 与整数）；numeric_tolerance 需明确 abs_tol/rel_tol。缺答案、冲突答案、题面提取案例、依赖未实现 checker 的案例跳过。只覆盖实际选定的输出通道，不声称捕获任意对象状态或交互。运行失败、超时、OOM、适配错误均不冒充正确性差异。

模型接口集中在 llm.py，目前支持 OpenAI 兼容 Chat Completions，流式/非流式均可；base_url、model、thinking、reasoning_effort、max_tokens 可配置。不支持 Responses 协议时明确报错，不偷偷切端点。切换不支持 DeepSeek 参数的服务时将 thinking/reasoning_effort 设为空字符串，必要时关闭 json_object；`[api.extra]` 可覆盖请求字段。新增协议只需扩充 llm.py 的请求构造与解析。

原始 TACO 默认启用两项固定来源配置：Codewars 用 single_wrapped 去除官方答案的单元素外包装；GFG 只有源码提供唯一受支持的 Solution 方法且没有顶层执行驱动时，使用该方法，并按逐行 JSON 解析参数和 JSON 解析答案。转换不依据运行结果调整，原始值和转换规则保留在案例中；不符合该格式就跳过。可删除 config 中对应 source_profiles 表关闭，或用 overrides 明确指定。已适配的旧 definition 不会重复应用来源配置。

尚未开始生成时，修改输入适配配置后可 `pipeline.py prepare --refresh` 重建准备材料；已有生成历史时拒绝刷新，应换 run_dir。候选来源和 seed 不变时仍为同一批题目。

## 输出与字段

状态和内部材料在 `runs/pilot/state.sqlite`。每次 export 新建目录，不覆盖已经填写的 CSV：

- `exports/<时间>/materials.jsonl`：完整私有材料，含变异源码、form 和生成请求关联。
- `exports/<时间>/blind/index.html`：可读试标页。
- `exports/<时间>/blind/records.jsonl`：公开证据。
- `exports/<时间>/blind/annotations.csv`：八字段和 notes；人工直接编辑。

只把 blind 目录交给行为标注者，保留人工初标后再连接 form。公开页不含变异源码、家族、diff、修改位置、stderr、栈或内部执行路径。导出器从白名单字段重新构造材料。

| 字段 | 当前输出 |
|---|---|
| form | 内部自动记录家族、规则、片段、位置、AST 修改；盲标表写“盲标阶段隐藏” |
| trigger_condition | `taco-mutation-light pipeline不支持`，人工按具体官方见证概括 |
| first_divergence | `taco-mutation-light pipeline不支持` |
| propagation | `taco-mutation-light pipeline不支持` |
| observable_consequence | 自动记录实际观察通道和双侧结果/异常差异 |
| oracle_type | 自动记录目标通道与比较方式；语义 requirement 写上述占位文字 |
| behavior_abstraction | `taco-mutation-light pipeline不支持`，人工填写测试需求 |
| label_status | 初始 unknown；人工改为 verified/ambiguous/unknown |

verified 仅表示本次情境下已填写的行为解释得到确认，不要求尚未采集的轨迹字段填满，不表示整个程序被证明正确。机器见证状态与人工标签状态分开。保留无见证的变异体及已执行案例，不称其为等价变异体。

## 代码与验证

日常源文件就是 config.toml、prompts/mutation.md、tasks.py、llm.py、mutation.py、runner.py、pipeline.py、state.py、export.py。另有 adapters 中 6 个小文件和单个 test_pipeline.py。

复制来源：adapters/interface.py 来自 motherdata/v112/adaptation.py；driver.py 来自 sandbox_driver.py（支持 stdin 正常 SystemExit(0)）；form.py 来自 form_core.py；probe.py 来自 isolation_probe.py；representation.py 提取 migration.py 的显式转换，删除题面提取及历史审核依赖。runner.py 复用旧容器配置、输出读取与限时执行思路，移除全局串行锁和阶段目录限制。运行不 import 旧项目。

```bash
.venv/bin/python -B test_pipeline.py
```

测试使用本地 HTTP 模拟响应和真实 Docker 执行，不调用付费模型、不读取真实凭据。验证真实六路 HTTP 重叠、六个目标容器重叠、适配器、补跑一次、二次失败保留、恢复不重发、早停和导出。测试证据保存于 `runs/selftest-*/verification.json`；合成示例导出带明确提示，不能充当真实试标数据。
