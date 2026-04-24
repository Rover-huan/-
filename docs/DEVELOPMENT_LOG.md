# AutoReport AI / SmartAnalyst 阶段性开发记录

本文档记录项目阶段性工程改动和验收结论，便于后续 AI 助手或开发者接手时快速了解近期优化背景。

## 2026-04-24 候选图有限并发性能优化

### 优化目标

本轮优化目标是解决分析阶段中候选图生成耗时过长的问题。

旧基线任务的关键耗时如下：

- 旧基线总耗时约 5 分 46 秒。
- 旧 `analysis.phase_completed` 约 4 分 42 秒。
- 旧候选图生成阶段墙钟约 4 分 21 秒。
- 主要瓶颈集中在 10 张候选图生成。

### 实现方案

本轮采用有限并发生成候选图，避免继续完全串行执行。

- 使用 `CHART_GENERATION_CONCURRENCY` 控制候选图生成并发数。
- 当前默认值为 2。
- 每次最多同时生成 2 张候选图。
- 完成一个候选图后再补位启动下一个候选图。
- 不改变 prompt 核心质量要求。
- 不改变候选图数量目标，仍然生成约 10 张候选图。
- 不改变“一 task 一图片 artifact、一卡一图”的规则。
- 不改变数据库结构、权限系统、部署脚本或 Celery 队列结构。

### 验收结果

只读验收使用的新任务：

- 最新 job：`f4387f190b8f413782afe17852aae2d9`
- 状态：`completed`
- 新总耗时约 4 分 25 秒。
- 总耗时相对旧基线下降约 23%。
- `analysis.phase_completed` 从约 4 分 42 秒降到约 2 分 38 秒，下降约 44%。
- 候选图生成阶段墙钟从约 4 分 21 秒降到约 2 分 15 秒，下降约 48%。

需要注意：并发后 10 张图各自 `duration_ms` 累加仍接近旧基线，这是正常现象；本轮优化节省的是候选图生成阶段的墙钟等待时间。

### 并发有效性证据

JobEvent 时间线显示本轮并发符合预期：

- Task 1 和 Task 2 几乎同时 started。
- 后续任务在前一个任务完成后补位启动。
- `analysis.chart_completed` 顺序出现乱序，例如 Task 8、Task 9、Task 10 早于 Task 7 completed。
- 这符合 `CHART_GENERATION_CONCURRENCY=2` 的有限并发行为。

### 副作用检查

本轮只读验收未发现明显副作用：

- `analysis.chart_started`：10 条。
- `analysis.chart_completed`：10 条。
- `analysis.chart_failed`：0 条。
- 未发现 timeout、rate limit、retry、self-healing、disallowed import 相关事件。
- `job_tasks` 数量完整，共 10 条。
- 10 个 chart storage 文件全部存在。
- `docx`、`ipynb`、`txt`、`zip` 报告产物全部存在。
- `docx` 和 `zip` 完整性检查通过。
- notebook 未发现明显 missing marker。

### 当前结论

本轮并发优化成功。

建议保留当前实现。默认并发 2 是保守且合理的值，能显著缩短分析阶段耗时，同时未在本轮验收中观察到限流、超时、候选图缺失或报告缺图问题。

暂不建议继续提高并发数。

### 下一步建议

- 连续使用同一份数据再跑 2-3 次，观察稳定性。
- 重点观察是否出现 DeepSeek 限流、timeout、`analysis.chart_failed` 或报告缺图。
- 如果连续多次稳定，再考虑下一轮优化。
- 下一轮优化可优先考虑更细粒度的 LLM 调用耗时观测、单图生成 prompt/token 优化，而不是直接提高并发数。

### 稳定性复测补充

并发优化和后续 `DataEmptyError` 最小修复完成后，使用同一批 2 个 Excel 文件连续跑了 3 次真实任务，3 次均 `completed`，最终报告均可正常生成。

| job_id | 总耗时 | 分析阶段耗时 | 候选图墙钟耗时 | chart_started | chart_completed | chart_failed | 产物检查 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `8f1da641f6ae47b38f22225a980b9f52` | 205.8s | 125.3s | 104.6s | 10 | 10 | 0 | chart/docx/ipynb/txt/zip 完整，docx 和 zip 校验通过 |
| `f01c57870ed8406ca1dd5b0ef5daa9d7` | 209.0s | 153.7s | 129.4s | 10 | 10 | 0 | chart/docx/ipynb/txt/zip 完整，docx 和 zip 校验通过 |
| `64c2698bd6e34aa894bf38241db19522` | 211.3s | 155.0s | 132.3s | 10 | 10 | 0 | chart/docx/ipynb/txt/zip 完整，docx 和 zip 校验通过 |

复测期间未发现：

- `DataEmptyError`
- DeepSeek `Content Exists Risk`
- timeout
- rate limit
- retry
- `analysis.chart_failed`
- 候选图文件缺失
- 报告引用不存在图表

当前补充结论：`CHART_GENERATION_CONCURRENCY=2` 在本轮同数据连续真实复测中表现稳定，建议保留。候选图并发优化阶段可以关闭，后续不建议直接提高并发数。

## 2026-04-24 DataEmptyError 最小稳定性修复

### 问题背景

候选图生成阶段曾出现一次真实任务失败：模型生成的图表代码连续 3 次把 `data_plot` 清洗或筛选为空，触发：

`DataEmptyError: 数据清洗后数据量为0，无法绘图。请检查数据过滤条件。`

只读诊断结论是：该问题更像 LLM 生成代码和 self-healing prompt 不够稳，不像 `CHART_GENERATION_CONCURRENCY=2` 导致的共享状态污染。

### 修复内容

本轮只围绕候选图执行器和错误分类做最小修复，没有回滚候选图并发优化，也没有修改并发调度逻辑。

- 加强 `DataEmptyError` repair prompt：
  - 明确要求放宽或移除导致空数据的筛选条件。
  - 明确禁止继续使用会产生空 `data_plot` 的 `str.contains`、过严 `dropna` 或错误列名假设。
  - 要求优先选择真实存在、非空、可数值转换的列。
  - 如果筛选后为空，退回到更宽松的数据选择或原始字段绘图。
  - 禁止再次返回会绘制空 dataframe 的代码。
- 增加安全 debug snapshot：
  - 只记录 dataframe shape、列名、每列非空计数、数值列候选摘要。
  - 不记录完整原始数据行。
  - 不记录 `df.head()`。
  - 不新增 generated code debug 文件落盘。
- 修正错误分类：
  - `DataEmptyError` 不再映射为 `file_parse_error`。
  - 新错误码为 `data_empty_after_filter`。
  - 错误类别为 `executor_error`。
  - 用户提示为：分析代码生成的筛选条件过严，导致可绘图数据为空。
- 补充测试：
  - 覆盖 `DataEmptyError` 能进入 repair 流程。
  - 覆盖 repair prompt 中包含放宽/移除导致空数据筛选条件等明确指令。
  - 覆盖 `DataEmptyError` 错误分类。
  - 覆盖安全 debug snapshot 不记录完整原始数据值。

### 验证结果

单测验证通过：

- `python -m compileall src/node3_executor.py service/error_mapper.py`
- `python -m pytest tests/test_executor_runner.py`
- `python -m pytest tests/test_api_regressions.py`

真实任务回归验证：

- 使用同一批触发问题的 2 个 Excel 文件连续跑 3 次真实任务。
- 3 次均成功完成 analysis 和 render。
- 每次均生成 10 条 `job_tasks`。
- 每次均有 10 条 `analysis.chart_started` 和 10 条 `analysis.chart_completed`。
- 每次 `analysis.chart_failed` 均为 0。
- 3 次均未再出现 `DataEmptyError`。
- 3 次最终报告产物 `docx`、`ipynb`、`txt`、`zip` 均完整。
- `docx` 和 `zip` 完整性检查通过。

当前结论：`DataEmptyError` 最小修复通过本轮真实回归验证。严格来说，本轮真实任务没有再次触发 `DataEmptyError` repair 分支；repair 分支已由单测覆盖，真实回归说明同批数据在当前实现下已稳定通过。

## 剩余风险记录

render 阶段曾出现一次 DeepSeek `Content Exists Risk`：

- 该问题发生在最终报告合成阶段，不发生在候选图生成阶段。
- 本轮 3 次稳定性复测暂未复现。
- 该问题应记录为后续要单独处理的模型风控 fallback 问题。
- 后续如果修复，应单独设计 render fallback、提示词改写或内容删减策略，不要和 chart 并发逻辑混在一起改。
