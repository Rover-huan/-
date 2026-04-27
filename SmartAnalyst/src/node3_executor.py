"""Node 3 executor for generating, running, and self-healing analysis code."""

from __future__ import annotations

import contextlib
import io
import json
import logging
import math
import os
import pickle
import re
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any, TypedDict

import matplotlib
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

matplotlib.use("Agg")

import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from src.node1_scanner import llm_caller
from service.config import get_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger().setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)

MAX_EXECUTION_ATTEMPTS = 3
CODE_BLOCK_PATTERN = re.compile(
    r"```(?:python)?\s*(?P<code>[\s\S]*?)\s*```",
    flags=re.IGNORECASE,
)
IMPORT_STATEMENT_PATTERN = re.compile(
    r"^\s*(?:import\s+[A-Za-z_][\w.]*|from\s+[A-Za-z_][\w.]*\s+import\s+)",
    flags=re.IGNORECASE,
)
REQUIRED_TASK_PLAN_KEYS = {
    "task_id",
    "question_zh",
    "analysis_type",
    "required_datasets",
    "x_axis_col",
    "y_axis_col",
    "x_axis_label_zh",
    "y_axis_label_zh",
}
PREP_START_MARKER = "# === PREP START ==="
PREP_END_MARKER = "# === PREP END ==="
PLOT_START_MARKER = "# === PLOT START ==="
PLOT_END_MARKER = "# === PLOT END ==="
DATA_EMPTY_ERROR_MESSAGE = (
    "\u6570\u636e\u6e05\u6d17\u540e\u6570\u636e\u91cf\u4e3a0\uff0c\u65e0\u6cd5\u7ed8\u56fe\u3002"
    "\u8bf7\u68c0\u67e5\u6570\u636e\u8fc7\u6ee4\u6761\u4ef6\u3002"
)
SAFE_DEBUG_MAX_COLUMNS = 40
SAFE_DEBUG_MAX_NUMERIC_CANDIDATES = 20
PLOT_SAFETY_REDLINE_PROMPT = """
【绘图强制安全纪律 - 绝对红线】：
1. 严禁使用 `plt.text`、`plt.annotate`、`ax.text` 等任何硬编码坐标的函数添加文本标签！
2. 严禁尝试在柱状图、折线图上直接标注数值！
3. 系统底层已开启静态代码扫描，任何包含 `plt.text` 或 `annotate` 的代码将被直接作为恶意代码拦截并导致任务失败。
4. 你只能使用 `plt.title`, `plt.xlabel`, `plt.ylabel`, `plt.legend` 来展示图表信息。
""".strip()
TYPE_CONVERSION_SAFETY_REDLINE_PROMPT = """
【Pandas 类型转换强制安全纪律 - 绝对红线】：
1. 严禁使用 `.astype(int)` 或 `.astype(float)` 对可能包含非数字字符、脏数据或 NaN 的列进行强转！这会导致不可恢复的 ValueError。
2. 只要涉及把字符串转换为数值，必须且只能使用 `pd.to_numeric(df['column_name'], errors='coerce')`。
3. 如果是在 `sort_values` 中提取数字排序，必须先安全转换：例如先新增一列 `df['month_num'] = pd.to_numeric(df['月份'].str.extract('(\\d+)')[0], errors='coerce')`，然后 `dropna`，最后再根据该列排序。绝对不允许在 lambda 表达式里裸调 `.astype(int)`！
""".strip()
DATA_CLEANING_GOLDEN_PATH_PROMPT = """
【数据清洗黄金法则 (Golden Rules for Data Cleaning)】:
1. **Be Conservative with `dropna()`:** Do NOT drop rows aggressively. If a column contains messy strings (like "January" mixed with numbers), do NOT filter out the string rows immediately if they represent valid data points.
2. **Handling Dates/Categoricals:** If a column represents months (e.g., '1月', 'January') and you need to sort it, do NOT try to extract numbers and cast to integers if the formats are mixed. Instead, treat it as a categorical variable.
   * *Example Strategy:* If the data contains strings like 'January', use those strings directly as labels for the X-axis. Do not force them into numeric types if it causes errors.
3. **Verify Data After Filtering:** Before plotting, the LLM must mentally verify that its `str.contains` or filtering logic will not result in an empty DataFrame. If you filter for '月' but the data uses 'January', the result will be empty. Look closely at the `df.head()` provided in the context to see the ACTUAL string formats.
4. **Resilience:** If you encounter a `DataEmptyError` in a previous attempt, you MUST completely change your filtering strategy. Look at the raw data sample provided and ensure your regex or string matching aligns with the actual content.
""".strip()
IMAGE_OUTPUT_SAFETY_REDLINE_PROMPT = """
【图片产出与保存强制纪律 - 绝对红线】：
1. 你的代码必须在最后一步明确调用 `plt.savefig(output_image_path, dpi=300, bbox_inches='tight')` 将图表保存到本地！
2. 绝对不能省略这一步，严禁使用硬编码的路径（如 'C:/xxx/xxx.png'），必须严格使用系统注入的 `output_image_path` 变量！
3. 严禁把 `plt.savefig` 包含在可能不会被执行到的 `if-else` 分支中。无论数据长什么样，你必须保证程序结束时，硬盘上确实生成了这张图。
""".strip()
SINGLE_CHART_SAFETY_REDLINE_PROMPT = """
【单图输出强制纪律 - 绝对红线】：
1. 每个候选图任务只能生成一个完整图表产物，并且只能调用一次 `plt.savefig(output_image_path, dpi=300, bbox_inches='tight')`。
2. 严禁生成多个独立 subplot 面板，严禁把多张独立图塞进同一张图片里。
3. 不要使用 `plt.subplot(...)`、`plt.subplots(1, 2)`、`plt.subplots(2, 1)`、`plt.subplots(2, 2)`、`fig.add_subplot(...)` 等多面板布局。
4. 如果需要表达多个指标，优先在同一个主图面板内使用分组柱状图、堆叠图、折线组合、散点颜色编码、图例等方式。
5. 为了保证图表质量，可以使用 legend、colorbar 或必要的辅助坐标轴（如共享同一图表区域的 twinx/twiny/secondary axis），但最终仍必须只有一个主图面板。
6. 保存到 `output_image_path` 的图片必须只对应一个候选图卡片。
""".strip()
MULTI_DATASET_SAFETY_REDLINE_PROMPT = """
【多表数据调用强制纪律 - 绝对红线】：
1. 当前环境**没有**名为 `df` 的全局变量！你只有一个名为 `datasets` 的字典。
2. 获取数据必须通过字典键值提取，例如：`df_A = datasets['file1.csv'].copy()`。不要瞎编文件名，必须严格使用任务分配给你的文件名！
3. 如果需要跨表分析，你必须自行使用 `pd.merge(df_A, df_B, on='公共列名', how='inner')` 进行数据对齐，合并前务必注意处理可能的数据类型不一致问题。
""".strip()
IMPORT_SAFETY_REDLINE_PROMPT = """
【导入语句强制纪律 - 绝对红线】：
1. 生成代码中禁止出现任何 `import ...` 或 `from ... import ...` 语句。
2. 不要导入 pandas、numpy、matplotlib、seaborn、math 或任何其他模块。
3. 执行器已经预加载常用对象，可以直接使用 `pd`、`np`、`plt`、`math`。
4. 只输出可执行分析绘图代码，不要输出 import 区域。
5. 如果需要某个模块能力，优先使用上述已存在对象或纯 Python 写法改写，绝对不要新增 import。
6. 不要因为不能 import 就降低图表复杂度或图表质量。
""".strip()

CODE_SYSTEM_PROMPT = f"""
You are a senior undergraduate data-analysis student writing homework code in Python.
Return executable Python code only.
Do not include explanations.
Do not include markdown outside a Python code block.
Use only the provided dataset dictionary `datasets`.
Do not read any files from disk.
Allowed preloaded objects: `pd`, `np`, `plt`, and `math`.

{IMPORT_SAFETY_REDLINE_PROMPT}

{MULTI_DATASET_SAFETY_REDLINE_PROMPT}

【绘图强制安全纪律 - 绝对红线】：
1. 严禁使用 `plt.text`、`plt.annotate`、`ax.text` 等任何硬编码坐标的函数添加文本标签！
2. 严禁尝试在柱状图、折线图上直接标注数值！
3. 系统底层已开启静态代码扫描，任何包含 `plt.text` 或 `annotate` 的代码将被直接作为恶意代码拦截并导致任务失败。
4. 你只能使用 `plt.title`, `plt.xlabel`, `plt.ylabel`, `plt.legend` 来展示图表信息。

{TYPE_CONVERSION_SAFETY_REDLINE_PROMPT}

{DATA_CLEANING_GOLDEN_PATH_PROMPT}

{IMAGE_OUTPUT_SAFETY_REDLINE_PROMPT}

{SINGLE_CHART_SAFETY_REDLINE_PROMPT}

The code must look like real student work, not a framework:
- Use flat script code instead of functions.
- Use student-like variable names such as `df_A`, `df_B`, `df_clean`, `data_plot`, `x_data`, `y_data`.
- The runtime injects `datasets`, `output_image_path`, `pd`, `plt`, `np`, and `math`. There is no global `df`.
- First load the task-assigned tables from `datasets`, then create a local working DataFrame named `df`.
- For single-table work, `df = datasets['assigned_file.csv'].copy()`.
- For multi-table work, load each assigned table, merge them into a local working DataFrame `df`, and then continue cleaning/plotting.
- Before any plotting, create `column_rename_map` and rename English or awkward multiline columns into concise professional Chinese names.
- The preprocessing block must look like a student's notebook cell and must include:
  `print(df.shape)`
  `print(df.info())`
  `print(df.head())`
  explicit column renaming with `df.rename(columns=column_rename_map)`
  explicit missing-value handling with `fillna(...)` or `dropna(...)`
- Before every `plt.savefig(...)` or `fig.savefig(...)`, make sure the plotting payload is not empty.
- Keep `data_plot` non-empty after cleaning/filtering. If you extract plotting vectors, keep `x_data` and `y_data` non-empty too.
- The code must define these variables:
  `analysis_result_text`
  `cleaning_summary_text`
  `problem_solution_text`
  `reflection_hint_text`
- The code must save the chart to the exact `output_image_path`.
- Do not use try/except.
- Do not define functions.
- Do not draw fallback error text on charts.

The code must include these exact section markers:
# === PREP START ===
# === PREP END ===
# === PLOT START ===
# === PLOT END ===

The code must begin with these exact Matplotlib settings:
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False
""".strip()

INITIAL_USER_PROMPT_TEMPLATE = """
请根据以下任务计划，为给定的 pandas DataFrame `df` 生成可直接执行的 Python 代码。

任务计划:
{task_plan_json}

DataFrame 列名:
{columns_json}

DataFrame 前 3 行样本:
{sample_rows_json}

必须严格遵守以下要求:
1. 只能使用当前已经存在的 `df`，不能重新读文件。
2. 必须先写字段重命名和清洗，再写绘图。
3. 字段重命名必须使用 `column_rename_map`，把英文列名或带换行的原始列名整理成规范中文列名。
4. 必须创建 `df_clean` 和 `data_plot` 这两个变量。
5. 在预处理部分必须原样包含:
   print(df.shape)
   print(df.info())
6. 必须用如下分段标记组织代码:
   {prep_start}
   {prep_end}
   {plot_start}
   {plot_end}
7. 必须把图保存到这个精确路径:
   {output_image_path}
8. 必须生成以下四个字符串变量:
   analysis_result_text
   cleaning_summary_text
   problem_solution_text
   reflection_hint_text
9. `problem_solution_text` 必须写成真实的数据处理困难与解决方式。
   如果没有缺失值，可以写字段名换行、单位符号、数值类型转换、分组口径统一等真实问题。
10. 代码风格要像学生作业，变量名自然，步骤清楚。
11. 不要写 try/except，不要写函数，不要输出解释性文字，只输出代码。
""".strip()

REPAIR_USER_PROMPT_TEMPLATE = """
下面这段 Python 代码执行失败了，请修复它并输出修复后的完整 Python 代码。

任务计划:
{task_plan_json}

DataFrame 列名:
{columns_json}

DataFrame 前 3 行样本:
{sample_rows_json}

报错信息:
{error_traceback}

上一版失败代码:
```python
{previous_code}
```

修复时必须继续遵守以下要求:
1. 只能使用当前已经存在的 `df`。
2. 必须保留字段重命名、清洗、探索输出和绘图四部分逻辑。
3. 必须包含:
   print(df.shape)
   print(df.info())
4. 必须继续使用:
   column_rename_map
   df_clean
   data_plot
5. 必须继续使用如下分段标记:
   {prep_start}
   {prep_end}
   {plot_start}
   {plot_end}
6. 必须继续生成:
   analysis_result_text
   cleaning_summary_text
   problem_solution_text
   reflection_hint_text
7. 图像必须保存到:
   {output_image_path}
8. 不要写 try/except，不要写函数，只输出代码。
""".strip()


SAFE_INITIAL_USER_PROMPT_TEMPLATE = """
请根据下面的任务计划，为当前已经存在的 pandas DataFrame `df` 生成可直接执行的 Python 代码。

任务计划:
{task_plan_json}

DataFrame 列名:
{columns_json}

DataFrame 前 3 行样例:
{sample_rows_json}

原始 df.head() 快照:
{raw_head_text}

必须严格遵守以下要求:
1. 只能使用当前已经存在的 `df`，不能重新读取任何文件。
2. 必须先写字段重命名和数据清洗，再写绘图。
3. 字段重命名必须使用 `column_rename_map`，把英文列名或不规范列名整理成专业中文列名。
4. 必须创建 `df_clean` 和 `data_plot` 这两个变量。
5. 在预处理部分必须原样包含:
   print(df.shape)
   print(df.info())
   print(df.head())
6. 必须使用如下分段标记组织代码:
   {prep_start}
   {prep_end}
   {plot_start}
   {plot_end}
7. 图像必须保存到这个精确路径:
   {output_image_path}
8. 必须生成以下四个字符串变量:
   analysis_result_text
   cleaning_summary_text
   problem_solution_text
   reflection_hint_text
9. `problem_solution_text` 必须写成真实的数据处理困难与解决方式。
10. 代码风格要像学生作业，变量名自然，步骤清楚。
11. 不要写 try/except，不要定义函数，只输出代码。
12. 绝对不要把数据过滤成空图。保存图像前，`data_plot` 不能是空数据；如果使用 `x_data` 和 `y_data`，它们也不能为空。
""".strip()

SAFE_REPAIR_USER_PROMPT_TEMPLATE = """
下面这段 Python 代码执行失败了，请修复它并输出修复后的完整 Python 代码。

任务计划:
{task_plan_json}

DataFrame 列名:
{columns_json}

DataFrame 前 3 行样例:
{sample_rows_json}

原始 df.head() 快照:
{raw_head_text}

报错信息:
{error_traceback}

上一版失败代码:
```python
{previous_code}
```

修复时必须继续遵守以下要求:
1. 只能使用当前已经存在的 `df`。
2. 必须保留字段重命名、数据清洗、探索输出和绘图这四部分逻辑。
3. 必须包含:
   print(df.shape)
   print(df.info())
   print(df.head())
4. 必须继续使用:
   column_rename_map
   df_clean
   data_plot
5. 必须继续使用如下分段标记:
   {prep_start}
   {prep_end}
   {plot_start}
   {plot_end}
6. 必须继续生成:
   analysis_result_text
   cleaning_summary_text
   problem_solution_text
   reflection_hint_text
7. 图像必须保存到:
   {output_image_path}
8. 不要写 try/except，不要定义函数，只输出代码。
9. 如果报错包含 `数据清洗后数据量为0，无法绘图。请检查数据过滤条件。`，你必须结合上面的原始 `df.head()` 快照修正筛选条件、分组方式或缺失值处理，绝不能再次生成空图。
""".strip()

STRICT_INITIAL_USER_PROMPT_TEMPLATE = """
Generate executable Python code for the existing dataset dictionary `datasets`.

Task plan:
{task_plan_json}

Assigned dataset file names:
{required_datasets_json}

Assigned dataset catalog:
{dataset_catalog_json}

Raw dataset head snapshots before cleaning:
{raw_head_text}

{multi_dataset_safety_redline}

{import_safety_redline}

{plot_safety_redline}

{type_conversion_safety_redline}

{data_cleaning_golden_path}

{image_output_safety_redline}

{single_chart_safety_redline}

Rules:
1. Use only the injected `datasets` dictionary. Do not read any files.
2. You must load only the datasets listed in `required_datasets`.
3. You must access real data with literal dataset-name lookups, exactly like:
   df = datasets['精确文件名'].copy()
   For a single-table task, the first working DataFrame line must be `df = datasets['the exact required dataset filename'].copy()`.
   For a multi-table task, load every required dataset with its literal file name before merging, for example:
   df1 = datasets['文件名1'].copy()
   df2 = datasets['文件名2'].copy()
4. Do not use `datasets.get(...)` instead of literal lookups. Do not use variable lookups such as `datasets[name]`, `datasets[dataset_name]`, or `next(iter(datasets.values()))` as the only dataset access.
5. Do not invent the main data with `pd.DataFrame(...)`, lists, dictionaries, or hard-coded rows. You may only create derived DataFrames from real loaded datasets.
6. Do not assume a global `df` already exists. Do not write explanation-only code. Every chart script must read at least one real dataset from `datasets`.
7. If the planned chart is unsuitable for the assigned data, still build a simple fallback chart from real columns in a real loaded dataset.
8. After loading or merging the assigned datasets, create a local working DataFrame named `df`.
9. Write preprocessing first, then plotting.
10. Create `column_rename_map` to rename English or awkward raw columns into professional Chinese column names.
11. You must create both `df_clean` and `data_plot`.
12. The preprocessing block must include these exact lines somewhere:
   print(df.shape)
   print(df.info())
   print(df.head())
13. Use these exact section markers:
   {prep_start}
   {prep_end}
   {plot_start}
   {plot_end}
14. You must save the chart with this exact call:
   plt.savefig(output_image_path, dpi=300, bbox_inches='tight')
15. You must define these strings:
   analysis_result_text
   cleaning_summary_text
   problem_solution_text
   reflection_hint_text
16. Make `problem_solution_text` describe a real data-cleaning difficulty and fix.
17. Do not write any `import ...` or `from ... import ...` statements. Use the preloaded `pd`, `np`, `plt`, and `math` objects directly.
18. Do not use try/except. Do not define functions. Output code only.
19. Never over-filter into an empty chart. Before saving, `data_plot` must still contain rows. If you use `x_data` and `y_data`, they must stay non-empty too.
""".strip()

STRICT_REPAIR_USER_PROMPT_TEMPLATE = """
The previous Python code failed. Return a fully repaired Python script for the same dataset dictionary `datasets`.

Task plan:
{task_plan_json}

Assigned dataset file names:
{required_datasets_json}

Assigned dataset catalog:
{dataset_catalog_json}

Raw dataset head snapshots before cleaning:
{raw_head_text}

Traceback:
{error_traceback}

{multi_dataset_safety_redline}

{import_safety_redline}

{plot_safety_redline}

{type_conversion_safety_redline}

{data_cleaning_golden_path}

{image_output_safety_redline}

{single_chart_safety_redline}

Previous failed code:
```python
{previous_code}
```

Repair requirements:
1. Use only the injected `datasets` dictionary and only the datasets listed in `required_datasets`.
2. Load the assigned datasets first, then create a local working DataFrame named `df` by copying a single table or merging multiple tables.
3. If the traceback contains "Generated code is missing required datasets access", this is a hard repair target:
   - Insert literal `datasets` access lines at the start of the repaired code, before preprocessing or plotting.
   - Use the real file names from `required_datasets`, not placeholders.
   - For one dataset, use exactly this shape:
     df = datasets['xxx.xlsx'].copy()
   - For multiple datasets, load each one with a literal file name first, for example:
     df1 = datasets['file1.xlsx'].copy()
     df2 = datasets['file2.xlsx'].copy()
   - Do not repair this error with `datasets.get(...)`, `datasets[name]`, `datasets[dataset_name]`, or `next(iter(datasets.values()))`.
   - Do not invent the main data with `pd.DataFrame(...)`, lists, dictionaries, or hard-coded rows.
   - If the original task is unsuitable for the data, build a simple fallback chart from real loaded dataset columns.
4. Keep the four-part flow: column renaming, cleaning, exploration output, plotting.
5. You must still include:
   print(df.shape)
   print(df.info())
   print(df.head())
6. You must still use:
   column_rename_map
   df_clean
   data_plot
7. You must keep these exact markers:
   {prep_start}
   {prep_end}
   {plot_start}
   {plot_end}
8. You must still define:
   analysis_result_text
   cleaning_summary_text
   problem_solution_text
   reflection_hint_text
9. You must save the chart with this exact call:
   plt.savefig(output_image_path, dpi=300, bbox_inches='tight')
10. If the traceback mentions "Generated code contains disallowed import statements", "disallowed import", "import statements", or "from ... import", delete every `import ...` and `from ... import ...` line from the repaired code.
11. Use the preloaded `pd`, `np`, `plt`, and `math` objects directly. If the previous code depended on an imported module, rewrite it with these objects or pure Python. Do not introduce any new import.
12. Keep the original chart intent and visual quality; do not simplify the chart just because imports are forbidden.
13. Do not use try/except. Do not define functions. Output code only.
14. If the traceback contains `DataEmptyError` or `{data_empty_error_message}`, treat it as a hard repair target:
   - Remove or relax the previous filtering condition that made `data_plot`, `x_data`, `y_data`, or `df_clean` empty.
   - Do not keep using `str.contains`, aggressive `dropna`, or assumed column names when they caused an empty `data_plot`.
   - Check that the working DataFrame is not empty before plotting.
   - Prefer real existing columns that are non-empty and can be converted with `pd.to_numeric(..., errors='coerce')`.
   - If a filtered DataFrame becomes empty, fall back to a broader selection or plot from the original valid fields.
   - Never return code that will plot an empty DataFrame again.
   - 修复时必须放宽或移除导致空数据的筛选条件，不要继续使用会产生空 `data_plot` 的 `str.contains`、过严 `dropna` 或错误列名假设。
   - 如果筛选后为空，必须退回到更宽松的数据选择或原始字段绘图，禁止再次返回会绘制空 dataframe 的代码。
15. If the traceback mentions "Each task must save exactly one chart panel", "Detected 2 axes", "multiple axes", or "subplot", inspect the previous plotting code:
   - If it created multiple independent subplot panels, merge the idea into one main chart panel.
   - Do not remove useful chart quality elements such as colorbar, legend, or a necessary shared auxiliary axis when they support one main chart.
   - Keep only one main chart panel and call `plt.savefig(output_image_path, dpi=300, bbox_inches='tight')` exactly once.
   - Before plotting, start from a clean figure with `plt.close('all')` and then create one figure for this task.
""".strip()


class ExecutionResult(TypedDict):
    """Successful execution result for one task."""

    task_id: int
    image_path: str
    analysis_text: str
    code_snippet: str
    prepare_code: str
    plot_code: str
    exploration_output: str
    preprocessing_code: str
    preprocessing_output_summary: str
    cleaning_summary: str
    problem_solution: str
    reflection_hint: str
    column_mapping: dict[str, str]


class ExecutorError(RuntimeError):
    """Raised when the executor cannot complete a task successfully."""


class DataEmptyError(ExecutorError):
    """Raised when cleaning/filtering removes all rows needed for plotting."""


def _validate_generated_code(code_str: str) -> None:
    """Reject generated code patterns that bypass the outer self-healing loop."""
    disallowed_patterns = {
        "import statements": re.compile(IMPORT_STATEMENT_PATTERN.pattern, flags=re.IGNORECASE | re.MULTILINE),
        "function definitions": re.compile(r"^\s*def\s+\w+\s*\(", flags=re.IGNORECASE | re.MULTILINE),
        "unsafe builtins": re.compile(r"\b(?:eval|exec|compile|__import__|input)\s*\(", flags=re.IGNORECASE),
        "file open calls": re.compile(r"\bopen\s*\(", flags=re.IGNORECASE),
        "filesystem/process/network modules": re.compile(
            r"\b(?:subprocess|socket|requests|httpx|urllib|shutil|pathlib|sys)\b",
            flags=re.IGNORECASE,
        ),
        "direct os access": re.compile(r"\bos\s*\.", flags=re.IGNORECASE),
        "dunder access": re.compile(r"__", flags=re.IGNORECASE),
        "manual df existence checks": re.compile(
            r"""["']df["']\s*(not\s+in|in)\s*(locals|globals)\(""",
            flags=re.IGNORECASE,
        ),
        "manual chart annotations": re.compile(r"\b(?:plt|ax)\.(?:text|annotate)\s*\(", flags=re.IGNORECASE),
    }
    required_patterns = {
        "datasets access": re.compile(r"datasets\s*\[\s*['\"][^'\"]+['\"]\s*\]"),
        "working df assignment": re.compile(r"^\s*df\s*=", flags=re.MULTILINE),
        "savefig output_image_path": re.compile(
            r"plt\.savefig\s*\(\s*output_image_path\s*,\s*dpi\s*=\s*300\s*,\s*bbox_inches\s*=\s*['\"]tight['\"]"
        ),
        "analysis_result_text": re.compile(r"^\s*analysis_result_text\s*=", flags=re.MULTILINE),
        "cleaning_summary_text": re.compile(r"^\s*cleaning_summary_text\s*=", flags=re.MULTILINE),
        "problem_solution_text": re.compile(r"^\s*problem_solution_text\s*=", flags=re.MULTILINE),
        "reflection_hint_text": re.compile(r"^\s*reflection_hint_text\s*=", flags=re.MULTILINE),
    }

    for description, pattern in disallowed_patterns.items():
        match = pattern.search(code_str)
        if match:
            if description == "import statements":
                line_number = code_str.count("\n", 0, match.start()) + 1
                import_line = code_str.splitlines()[line_number - 1].strip()
                raise ExecutorError(
                    "Generated code contains disallowed import statements. "
                    f"Disallowed import on line {line_number}: {import_line}. "
                    "Remove all import statements and use injected `pd`, `np`, `plt`, and `math` objects."
                )
            raise ExecutorError(
                f"Generated code contains disallowed {description}."
            )

    for description, pattern in required_patterns.items():
        if not pattern.search(code_str):
            raise ExecutorError(f"Generated code is missing required {description}.")

    if re.search(r"\btry\s*:", code_str, flags=re.IGNORECASE):
        LOGGER.warning(
            "Generated code contains try/except blocks. Allowing execution and relying on outer safeguards."
        )


SAFE_IMPORT_PATTERNS = tuple(
    re.compile(pattern, flags=re.IGNORECASE)
    for pattern in (
        r"^import\s+pandas\s+as\s+pd\s*$",
        r"^import\s+numpy\s+as\s+np\s*$",
        r"^import\s+matplotlib\.pyplot\s+as\s+plt\s*$",
        r"^from\s+matplotlib\s+import\s+pyplot\s+as\s+plt\s*$",
        r"^import\s+math\s*$",
        r"^from\s+__future__\s+import\s+annotations\s*$",
    )
)


def _normalize_generated_code(code_str: str) -> str:
    """Remove harmless duplicate imports for objects already injected by the runner."""
    normalized_lines: list[str] = []
    removed_imports: list[str] = []

    for line in code_str.splitlines():
        if any(pattern.match(line) for pattern in SAFE_IMPORT_PATTERNS):
            removed_imports.append(line.strip())
            continue
        normalized_lines.append(line)

    if removed_imports:
        LOGGER.info("Removed duplicate generated imports: %s", "; ".join(removed_imports))

    return "\n".join(normalized_lines).strip()


def extract_python_code(raw_text: str) -> str:
    """Extract Python code from an LLM response, tolerating fenced or raw code."""
    if not raw_text or not raw_text.strip():
        raise ValueError("LLM returned an empty code response.")

    cleaned_text = raw_text.strip()
    fenced_match = CODE_BLOCK_PATTERN.search(cleaned_text)
    if fenced_match:
        code = fenced_match.group("code").strip()
    else:
        code = cleaned_text

    if code.lower().startswith("python\n"):
        code = code.split("\n", 1)[1].strip()

    if not code:
        raise ValueError("Failed to extract executable Python code from the LLM response.")

    return code


def _extract_section(code_str: str, start_marker: str, end_marker: str) -> str:
    """Extract code between two explicit section markers."""
    pattern = re.compile(
        rf"{re.escape(start_marker)}\s*\n(?P<section>[\s\S]*?)\n{re.escape(end_marker)}",
        flags=re.MULTILINE,
    )
    match = pattern.search(code_str)
    if not match:
        raise ExecutorError(f"Failed to extract code section between {start_marker} and {end_marker}.")
    return match.group("section").strip()


def _validate_prepare_section(prepare_code: str) -> None:
    """Ensure the preprocessing block is present; product flow should not fail on style omissions."""
    if not prepare_code.strip():
        raise ExecutorError("Generated preprocessing block is empty.")


def _summarize_preprocessing_output(exploration_output: str) -> str:
    """Keep a notebook-friendly snapshot of the preprocessing console output."""
    cleaned_lines = [line.rstrip() for line in exploration_output.splitlines()]
    meaningful_lines = [line for line in cleaned_lines if line.strip()]
    if not meaningful_lines:
        return "No preprocessing console output was captured."
    return "\n".join(meaningful_lines[:80]).strip()


def _is_empty_plot_object(value: Any) -> bool:
    """Return True when a plotting payload is empty and should block rendering."""
    if value is None:
        return True

    if isinstance(value, (pd.DataFrame, pd.Series, pd.Index)):
        return value.empty

    if isinstance(value, np.ndarray):
        return value.size == 0

    if hasattr(value, "empty"):
        try:
            return bool(value.empty)
        except Exception:  # pragma: no cover - defensive fallback
            pass

    try:
        return len(value) == 0
    except TypeError:
        return False


def _summarize_dataframe_for_debug(dataframe: pd.DataFrame) -> dict[str, Any]:
    """Summarize DataFrame structure without row values or raw samples."""
    columns = [str(column) for column in dataframe.columns[:SAFE_DEBUG_MAX_COLUMNS]]
    non_null_counts: list[dict[str, Any]] = []
    numeric_candidates: list[dict[str, Any]] = []

    for column_index, column_name in enumerate(dataframe.columns[:SAFE_DEBUG_MAX_COLUMNS]):
        column_label = str(column_name)
        column_data = dataframe.iloc[:, column_index]
        non_null_count = int(column_data.notna().sum())
        non_null_counts.append(
            {
                "column": column_label,
                "non_null_count": non_null_count,
            }
        )

        converted = pd.to_numeric(column_data, errors="coerce")
        numeric_non_null_count = int(converted.notna().sum())
        if numeric_non_null_count > 0 and len(numeric_candidates) < SAFE_DEBUG_MAX_NUMERIC_CANDIDATES:
            numeric_candidates.append(
                {
                    "column": column_label,
                    "numeric_non_null_count": numeric_non_null_count,
                    "row_count": int(len(column_data)),
                }
            )

    return {
        "shape": [int(dataframe.shape[0]), int(dataframe.shape[1])],
        "columns": columns,
        "column_count": int(dataframe.shape[1]),
        "columns_truncated": int(dataframe.shape[1]) > SAFE_DEBUG_MAX_COLUMNS,
        "non_null_counts": non_null_counts,
        "numeric_column_candidates": numeric_candidates,
    }


def _summarize_vector_for_debug(value: Any) -> dict[str, Any]:
    """Summarize plotting vectors without exposing values."""
    summary: dict[str, Any] = {"type": type(value).__name__}
    try:
        summary["length"] = int(len(value))
    except TypeError:
        summary["length"] = None
    summary["is_empty"] = bool(_is_empty_plot_object(value))
    return summary


def _build_safe_debug_snapshot(exec_context: dict[str, Any], empty_target: str | None = None) -> dict[str, Any]:
    """Build non-sensitive debug metadata for empty plotting payload failures."""
    snapshot: dict[str, Any] = {
        "empty_target": empty_target,
        "frames": {},
        "vectors": {},
    }

    for frame_name in ("df", "df_clean", "data_plot"):
        frame_value = exec_context.get(frame_name)
        if isinstance(frame_value, pd.DataFrame):
            snapshot["frames"][frame_name] = _summarize_dataframe_for_debug(frame_value)

    for vector_name in ("x_data", "y_data"):
        if vector_name in exec_context:
            snapshot["vectors"][vector_name] = _summarize_vector_for_debug(exec_context.get(vector_name))

    return snapshot


def _raise_data_empty_error(exec_context: dict[str, Any], empty_target: str) -> None:
    """Raise DataEmptyError with a safe structural snapshot for repair prompts."""
    snapshot = _build_safe_debug_snapshot(exec_context, empty_target=empty_target)
    snapshot_text = json.dumps(snapshot, ensure_ascii=False, default=str)
    LOGGER.warning(
        "Detected empty `%s` before savefig; blocking blank chart output. Safe debug snapshot: %s",
        empty_target,
        snapshot_text,
    )
    raise DataEmptyError(f"{DATA_EMPTY_ERROR_MESSAGE} Safe debug snapshot: {snapshot_text}")


def _validate_plot_payload(exec_context: dict[str, Any]) -> None:
    """Block chart rendering when the cleaned plotting payload is empty."""
    if "data_plot" in exec_context:
        if _is_empty_plot_object(exec_context.get("data_plot")):
            _raise_data_empty_error(exec_context, "data_plot")
        return

    has_x_data = "x_data" in exec_context
    has_y_data = "y_data" in exec_context
    if has_x_data and has_y_data:
        if _is_empty_plot_object(exec_context.get("x_data")) or _is_empty_plot_object(
            exec_context.get("y_data")
        ):
            _raise_data_empty_error(exec_context, "x_data/y_data")
        return

    if "df_clean" in exec_context and _is_empty_plot_object(exec_context.get("df_clean")):
        _raise_data_empty_error(exec_context, "df_clean")


def _axis_bounds(axis: Any) -> tuple[float, float, float, float]:
    """Return an axis bounding box in figure-relative coordinates."""
    bounds = axis.get_position().bounds
    return float(bounds[0]), float(bounds[1]), float(bounds[2]), float(bounds[3])


def _axis_area(axis: Any) -> float:
    _, _, width, height = _axis_bounds(axis)
    return max(width, 0.0) * max(height, 0.0)


def _intersection_area(first_axis: Any, second_axis: Any) -> float:
    first_x, first_y, first_width, first_height = _axis_bounds(first_axis)
    second_x, second_y, second_width, second_height = _axis_bounds(second_axis)
    left = max(first_x, second_x)
    right = min(first_x + first_width, second_x + second_width)
    bottom = max(first_y, second_y)
    top = min(first_y + first_height, second_y + second_height)
    return max(right - left, 0.0) * max(top - bottom, 0.0)


def _axis_overlap_ratio(first_axis: Any, second_axis: Any) -> float:
    smaller_area = min(_axis_area(first_axis), _axis_area(second_axis))
    if smaller_area <= 0:
        return 0.0
    return _intersection_area(first_axis, second_axis) / smaller_area


def _is_colorbar_axis(axis: Any) -> bool:
    """Return True for Matplotlib-managed colorbar axes."""
    return axis.get_label() == "<colorbar>" or getattr(axis, "_colorbar", None) is not None


def _is_shared_panel_auxiliary_axis(axis: Any, main_axis: Any) -> bool:
    """Allow twinx/twiny/secondary axes that occupy the same chart panel."""
    return _axis_overlap_ratio(axis, main_axis) >= 0.85


def _validate_single_chart_figure(figure: Figure) -> None:
    """Ensure each saved image represents one chart product, not multiple panels."""
    axes = [axis for axis in (getattr(figure, "axes", []) or []) if axis.get_visible()]
    if not axes:
        raise ExecutorError("Each task must save exactly one chart panel. No visible axes were detected.")
    if len(axes) == 1:
        return

    non_colorbar_axes = [axis for axis in axes if not _is_colorbar_axis(axis)]
    if not non_colorbar_axes:
        raise ExecutorError("Each task must save exactly one chart panel. Only auxiliary axes were detected.")

    main_axis = max(non_colorbar_axes, key=_axis_area)
    independent_axes = [
        axis
        for axis in non_colorbar_axes
        if axis is not main_axis and not _is_shared_panel_auxiliary_axis(axis, main_axis)
    ]
    if independent_axes:
        raise ExecutorError(
            "Each task must save exactly one chart panel. Detected multiple independent chart panels; "
            "please merge them into one main chart panel. Colorbar and shared auxiliary axes may be kept."
        )


def _validate_savefig_target(args: tuple[Any, ...], exec_context: dict[str, Any]) -> None:
    """Ensure generated code writes only the expected chart artifact."""
    if not args:
        raise ExecutorError("Each task must save exactly one chart artifact to output_image_path.")

    target = args[0]
    expected_raw = exec_context.get("output_image_path")
    if not isinstance(target, (str, os.PathLike)) or not isinstance(expected_raw, (str, os.PathLike)):
        raise ExecutorError("Each task must save exactly one chart artifact to output_image_path.")

    target_path = Path(target).resolve()
    expected_path = Path(expected_raw).resolve()
    if target_path != expected_path:
        raise ExecutorError(
            "Each task must save exactly one chart artifact to the provided output_image_path. "
            "Do not save extra files or use a custom image path."
        )


def _install_safe_savefig(exec_context: dict[str, Any]) -> Any:
    """Patch savefig so every render attempt validates the plotting payload first."""
    original_plt_savefig = plt.savefig
    original_figure_savefig = Figure.savefig
    save_state = {"count": 0, "inside_pyplot_savefig": False}

    def _validate_savefig_call(args: tuple[Any, ...]) -> None:
        _validate_savefig_target(args, exec_context)
        save_state["count"] += 1
        if save_state["count"] > 1:
            raise ExecutorError(
                "Each task must save exactly one chart artifact. Multiple savefig calls were detected."
            )

    def _checked_pyplot_savefig(*args: Any, **kwargs: Any) -> Any:
        _validate_savefig_call(args)
        _validate_plot_payload(exec_context)
        _validate_single_chart_figure(plt.gcf())
        save_state["inside_pyplot_savefig"] = True
        try:
            return original_plt_savefig(*args, **kwargs)
        finally:
            save_state["inside_pyplot_savefig"] = False

    def _checked_figure_savefig(self: Figure, *args: Any, **kwargs: Any) -> Any:
        if save_state["inside_pyplot_savefig"]:
            return original_figure_savefig(self, *args, **kwargs)
        _validate_savefig_call(args)
        _validate_plot_payload(exec_context)
        _validate_single_chart_figure(self)
        return original_figure_savefig(self, *args, **kwargs)

    plt.savefig = _checked_pyplot_savefig  # type: ignore[assignment]
    Figure.savefig = _checked_figure_savefig  # type: ignore[assignment]

    def _restore() -> None:
        plt.savefig = original_plt_savefig  # type: ignore[assignment]
        Figure.savefig = original_figure_savefig  # type: ignore[assignment]

    return _restore


def _normalize_column_mapping(raw_mapping: Any) -> dict[str, str]:
    """Normalize the exposed rename map from the executed student code."""
    if not isinstance(raw_mapping, dict):
        return {}

    normalized_mapping: dict[str, str] = {}
    for key, value in raw_mapping.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        if key.strip() and value.strip():
            normalized_mapping[key.strip()] = value.strip()
    return normalized_mapping


def _extract_section_or_fallback(code_str: str, start_marker: str, end_marker: str) -> str:
    """Return one explicit code section when possible, otherwise fall back to the full code."""
    try:
        return _extract_section(code_str, start_marker, end_marker)
    except ExecutorError:
        LOGGER.warning(
            "Generated code is missing section markers between %s and %s. Falling back to the full script.",
            start_marker,
            end_marker,
        )
        return code_str.strip()


def _build_dataframe_console_snapshot(dataframe: pd.DataFrame) -> str:
    """Synthesize notebook-style stdout when the LLM did not print exploration output."""
    info_buffer = io.StringIO()
    dataframe.info(buf=info_buffer)
    if dataframe.empty:
        head_text = "<empty dataframe>"
    else:
        head_text = dataframe.head().to_string(index=False)
    return "\n".join(
        [
            str(dataframe.shape),
            info_buffer.getvalue().strip(),
            head_text,
        ]
    ).strip()


def _build_fallback_exploration_output(exec_context: dict[str, Any]) -> str:
    """Build a best-effort exploration snapshot from the runtime context."""
    for candidate_name in ("data_plot", "df_clean", "df"):
        candidate = exec_context.get(candidate_name)
        if isinstance(candidate, pd.DataFrame):
            return _build_dataframe_console_snapshot(candidate)

    return "Execution completed without explicit preprocessing console output."


def _build_default_result_texts(task_plan: dict[str, Any]) -> dict[str, str]:
    """Provide product-safe fallback narrative fields when the generated code omits them."""
    question = str(task_plan.get("question_zh", "")).strip() or "当前分析任务"
    return {
        "analysis_result_text": f"已完成“{question}”的图表分析，请结合生成图像查看核心结果。",
        "cleaning_summary_text": "已完成基础字段整理、数据清洗和绘图前检查。",
        "problem_solution_text": "系统执行成功，但模型未返回完整的问题处理说明，已使用默认摘要保底。",
        "reflection_hint_text": "该图表结果可作为后续报告撰写与人工复核的参考依据。",
    }


def _coerce_text_result(value: Any, fallback: str) -> str:
    """Return a non-empty string result, falling back when the generated code omitted it."""
    if isinstance(value, str) and value.strip():
        return value.strip()
    return fallback


def _collect_execution_outputs(
    *,
    task_plan: dict[str, Any],
    exec_context: dict[str, Any],
    exploration_output: str,
) -> dict[str, Any]:
    """Normalize values produced by generated analysis code."""
    if not exploration_output:
        exploration_output = _build_fallback_exploration_output(exec_context)

    default_result_texts = _build_default_result_texts(task_plan)
    return {
        "exploration_output": exploration_output,
        "analysis_result_text": _coerce_text_result(
            exec_context.get("analysis_result_text"),
            default_result_texts["analysis_result_text"],
        ),
        "cleaning_summary_text": _coerce_text_result(
            exec_context.get("cleaning_summary_text"),
            default_result_texts["cleaning_summary_text"],
        ),
        "problem_solution_text": _coerce_text_result(
            exec_context.get("problem_solution_text"),
            default_result_texts["problem_solution_text"],
        ),
        "reflection_hint_text": _coerce_text_result(
            exec_context.get("reflection_hint_text"),
            default_result_texts["reflection_hint_text"],
        ),
        "column_rename_map": _normalize_column_mapping(exec_context.get("column_rename_map")),
    }


def _summarize_exception_chain(exc: BaseException) -> str:
    """Flatten one exception chain into a concise one-line summary."""
    parts: list[str] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        text = str(current).strip()
        if text and text not in parts:
            parts.append(text)
        current = current.__cause__ or current.__context__

    if not parts:
        return type(exc).__name__
    return " | ".join(parts[:4])


def _validate_inputs(datasets: dict[str, pd.DataFrame], task_plan: dict[str, Any], output_dir: Path) -> None:
    """Validate executor inputs before making an LLM call."""
    if not isinstance(datasets, dict) or not datasets:
        raise ValueError("datasets must be a non-empty dictionary of pandas DataFrames.")

    for dataset_name, dataframe in datasets.items():
        if not isinstance(dataset_name, str) or not dataset_name.strip():
            raise ValueError("datasets keys must be non-empty strings.")
        if not isinstance(dataframe, pd.DataFrame):
            raise ValueError(f"datasets[{dataset_name!r}] must be a pandas DataFrame.")

    if not isinstance(task_plan, dict):
        raise ValueError("task_plan must be a dictionary.")

    missing_keys = REQUIRED_TASK_PLAN_KEYS.difference(task_plan.keys())
    if missing_keys:
        raise ValueError(f"task_plan is missing required keys: {sorted(missing_keys)}")

    task_id = task_plan.get("task_id")
    if not isinstance(task_id, int):
        raise ValueError("task_plan['task_id'] must be an integer.")

    required_datasets = task_plan.get("required_datasets")
    if not isinstance(required_datasets, list) or not required_datasets:
        raise ValueError("task_plan['required_datasets'] must be a non-empty list.")

    unknown_datasets = [
        str(dataset_name)
        for dataset_name in required_datasets
        if str(dataset_name) not in datasets
    ]
    if unknown_datasets:
        raise ValueError(
            "task_plan['required_datasets'] contains unknown datasets: "
            f"{sorted(unknown_datasets)}"
        )

    if not str(output_dir):
        raise ValueError("output_dir must be a valid path.")


def _build_prompt_context(
    datasets: dict[str, pd.DataFrame],
    task_plan: dict[str, Any],
    output_image_path: str,
) -> dict[str, str]:
    """Build shared JSON context for the LLM prompts."""
    required_dataset_names = [
        str(dataset_name).strip()
        for dataset_name in task_plan.get("required_datasets", [])
        if str(dataset_name).strip()
    ]
    dataset_catalog: list[dict[str, Any]] = []
    raw_head_blocks: list[str] = []
    merged_columns: list[str] = []

    for dataset_name in required_dataset_names:
        dataframe = datasets[dataset_name]
        dataset_columns = [str(column) for column in dataframe.columns]
        merged_columns.extend(dataset_columns)
        dataset_catalog.append(
            {
                "dataset_name": dataset_name,
                "shape": [int(dimension) for dimension in dataframe.shape],
                "columns": dataset_columns,
                "sample_rows": dataframe.head(3).to_dict(orient="records"),
            }
        )
        raw_head_blocks.append(
            f"=== {dataset_name} ===\n"
            + (dataframe.head().to_string(index=False) if not dataframe.empty else "<empty dataframe>")
        )

    return {
        "task_plan_json": json.dumps(task_plan, ensure_ascii=False, indent=2),
        "columns_json": json.dumps(sorted(set(merged_columns)), ensure_ascii=False),
        "sample_rows_json": json.dumps(dataset_catalog, ensure_ascii=False, indent=2, default=str),
        "required_datasets_json": json.dumps(required_dataset_names, ensure_ascii=False, indent=2),
        "dataset_catalog_json": json.dumps(dataset_catalog, ensure_ascii=False, indent=2, default=str),
        "raw_head_text": "\n\n".join(raw_head_blocks),
        "multi_dataset_safety_redline": MULTI_DATASET_SAFETY_REDLINE_PROMPT,
        "import_safety_redline": IMPORT_SAFETY_REDLINE_PROMPT,
        "plot_safety_redline": PLOT_SAFETY_REDLINE_PROMPT,
        "type_conversion_safety_redline": TYPE_CONVERSION_SAFETY_REDLINE_PROMPT,
        "data_cleaning_golden_path": DATA_CLEANING_GOLDEN_PATH_PROMPT,
        "image_output_safety_redline": IMAGE_OUTPUT_SAFETY_REDLINE_PROMPT,
        "single_chart_safety_redline": SINGLE_CHART_SAFETY_REDLINE_PROMPT,
        "data_empty_error_message": DATA_EMPTY_ERROR_MESSAGE,
        "output_image_path": output_image_path,
        "prep_start": PREP_START_MARKER,
        "prep_end": PREP_END_MARKER,
        "plot_start": PLOT_START_MARKER,
        "plot_end": PLOT_END_MARKER,
    }


def _build_initial_prompt(
    datasets: dict[str, pd.DataFrame],
    task_plan: dict[str, Any],
    output_image_path: str,
) -> str:
    """Build the initial code-generation prompt."""
    context = _build_prompt_context(
        datasets=datasets,
        task_plan=task_plan,
        output_image_path=output_image_path,
    )
    return STRICT_INITIAL_USER_PROMPT_TEMPLATE.format(**context)


def _build_repair_prompt(
    datasets: dict[str, pd.DataFrame],
    task_plan: dict[str, Any],
    output_image_path: str,
    error_traceback: str,
    previous_code: str,
) -> str:
    """Build the repair prompt using the exact traceback from the failed attempt."""
    context = _build_prompt_context(
        datasets=datasets,
        task_plan=task_plan,
        output_image_path=output_image_path,
    )
    context["error_traceback"] = error_traceback
    context["previous_code"] = previous_code
    return STRICT_REPAIR_USER_PROMPT_TEMPLATE.format(**context)


def _prepare_output_path(output_dir: Path, task_id: int) -> str:
    """Ensure the output directory exists and return the image path for this task."""
    os.makedirs(output_dir, exist_ok=True)
    return str((output_dir / f"task_{task_id}.png").resolve())


def _cleanup_previous_output(output_image_path: str) -> None:
    """Remove a stale image before retrying, preventing false success signals."""
    if os.path.exists(output_image_path):
        os.remove(output_image_path)


def _execute_generated_code_inprocess(
    *,
    code_str: str,
    datasets: dict[str, pd.DataFrame],
    task_plan: dict[str, Any],
    output_image_path: str,
) -> dict[str, Any]:
    """Execute generated code in the current process for local debugging."""
    exec_context: dict[str, Any] = {
        "datasets": datasets,
        "output_image_path": output_image_path,
        "plt": plt,
        "pd": pd,
        "np": np,
        "math": math,
    }
    plt.close("all")
    restore_savefig = _install_safe_savefig(exec_context)
    stdout_buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout_buffer):
            exec(code_str, exec_context)
    finally:
        restore_savefig()

    return _collect_execution_outputs(
        task_plan=task_plan,
        exec_context=exec_context,
        exploration_output=stdout_buffer.getvalue().strip(),
    )


def _build_runner_env(runner_dir: Path) -> dict[str, str]:
    """Build a minimal environment for the generated-code runner."""
    safe_env: dict[str, str] = {}
    for key in ("PATH", "SYSTEMROOT", "COMSPEC", "TEMP", "TMP", "HOME", "USERPROFILE"):
        value = os.environ.get(key)
        if value:
            safe_env[key] = value
    safe_env["PYTHONPATH"] = str(PROJECT_ROOT)
    safe_env["PYTHONNOUSERSITE"] = "1"
    safe_env["MPLCONFIGDIR"] = str((runner_dir / "mpl").resolve())
    safe_env.setdefault("MPLBACKEND", "Agg")
    return safe_env


def _execute_generated_code_subprocess(
    *,
    code_str: str,
    datasets: dict[str, pd.DataFrame],
    task_plan: dict[str, Any],
    output_image_path: str,
) -> dict[str, Any]:
    """Execute generated code in an isolated child process with a timeout."""
    settings = get_settings()
    task_id = int(task_plan["task_id"])
    runner_dir = (Path(output_image_path).parent / ".runner").resolve()
    runner_dir.mkdir(parents=True, exist_ok=True)
    input_path = runner_dir / f"task_{task_id}_input.pkl"
    output_path = runner_dir / f"task_{task_id}_output.pkl"

    with input_path.open("wb") as file_obj:
        pickle.dump(
            {
                "code_str": code_str,
                "datasets": datasets,
                "task_plan": task_plan,
                "output_image_path": output_image_path,
            },
            file_obj,
            protocol=pickle.HIGHEST_PROTOCOL,
        )

    if output_path.exists():
        output_path.unlink()

    command = [sys.executable, "-m", "src.runner_subprocess", str(input_path), str(output_path)]
    try:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            env=_build_runner_env(runner_dir),
            timeout=max(settings.runner_timeout_seconds, 1),
            capture_output=True,
            text=True,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise ExecutorError(
            f"Generated code runner timed out after {settings.runner_timeout_seconds} seconds."
        ) from exc

    if not output_path.exists():
        raise ExecutorError(
            "Generated code runner did not return an output payload. "
            f"Exit code: {completed.returncode}. stderr: {completed.stderr.strip()}"
        )

    with output_path.open("rb") as file_obj:
        result = pickle.load(file_obj)

    if not isinstance(result, dict) or not result.get("ok"):
        traceback_text = result.get("traceback") if isinstance(result, dict) else ""
        raise ExecutorError(
            "Generated code runner failed. "
            f"Exit code: {completed.returncode}. {str(traceback_text).strip()}"
        )

    return dict(result["outputs"])


def _execute_generated_code(
    *,
    code_str: str,
    datasets: dict[str, pd.DataFrame],
    task_plan: dict[str, Any],
    output_image_path: str,
) -> dict[str, Any]:
    """Execute generated code using the configured runner mode."""
    if get_settings().runner_mode == "inprocess":
        return _execute_generated_code_inprocess(
            code_str=code_str,
            datasets=datasets,
            task_plan=task_plan,
            output_image_path=output_image_path,
        )

    return _execute_generated_code_subprocess(
        code_str=code_str,
        datasets=datasets,
        task_plan=task_plan,
        output_image_path=output_image_path,
    )


def execute_task(
    datasets: dict[str, pd.DataFrame],
    task_plan: dict[str, Any],
    output_dir: str | Path,
) -> ExecutionResult:
    """Generate, execute, and self-heal Python analysis code for one plan item."""
    output_dir_path = Path(output_dir)
    _validate_inputs(datasets=datasets, task_plan=task_plan, output_dir=output_dir_path)

    task_id = int(task_plan["task_id"])
    output_image_path = _prepare_output_path(output_dir_path, task_id)
    LOGGER.info("Starting task execution for task_id=%s", task_id)

    attempt = 1
    previous_code = ""
    previous_traceback = ""
    last_exception: Exception | None = None
    attempt_failure_summaries: list[str] = []

    while attempt <= MAX_EXECUTION_ATTEMPTS:
        LOGGER.info(
            "Task %s execution attempt %s/%s",
            task_id,
            attempt,
            MAX_EXECUTION_ATTEMPTS,
        )

        try:
            code_str = ""
            _cleanup_previous_output(output_image_path)

            if attempt == 1:
                prompt = _build_initial_prompt(
                    datasets=datasets,
                    task_plan=task_plan,
                    output_image_path=output_image_path,
                )
            else:
                prompt = _build_repair_prompt(
                    datasets=datasets,
                    task_plan=task_plan,
                    output_image_path=output_image_path,
                    error_traceback=previous_traceback,
                    previous_code=previous_code,
                )

            raw_response = llm_caller(prompt=prompt, system_prompt=CODE_SYSTEM_PROMPT)
            code_str = _normalize_generated_code(extract_python_code(raw_response))
            _validate_generated_code(code_str)

            execution_outputs = _execute_generated_code(
                code_str=code_str,
                datasets=datasets,
                task_plan=task_plan,
                output_image_path=output_image_path,
            )

            if not os.path.exists(output_image_path):
                raise ExecutorError(
                    f"Generated code completed without creating the expected image: {output_image_path}"
                )

            prepare_code = _extract_section_or_fallback(code_str, PREP_START_MARKER, PREP_END_MARKER)
            plot_code = _extract_section_or_fallback(code_str, PLOT_START_MARKER, PLOT_END_MARKER)
            _validate_prepare_section(prepare_code)
            exploration_output = str(execution_outputs["exploration_output"])
            preprocessing_output_summary = _summarize_preprocessing_output(exploration_output)

            LOGGER.info("Task %s executed successfully on attempt %s", task_id, attempt)
            return {
                "task_id": task_id,
                "image_path": output_image_path,
                "analysis_text": str(execution_outputs["analysis_result_text"]).strip(),
                "code_snippet": code_str.strip(),
                "prepare_code": prepare_code,
                "plot_code": plot_code,
                "exploration_output": exploration_output,
                "preprocessing_code": prepare_code,
                "preprocessing_output_summary": preprocessing_output_summary,
                "cleaning_summary": str(execution_outputs["cleaning_summary_text"]).strip(),
                "problem_solution": str(execution_outputs["problem_solution_text"]).strip(),
                "reflection_hint": str(execution_outputs["reflection_hint_text"]).strip(),
                "column_mapping": dict(execution_outputs["column_rename_map"]),
            }
        except Exception as exc:
            last_exception = exc
            previous_traceback = traceback.format_exc()
            previous_code = code_str or previous_code or ""
            attempt_failure_summaries.append(
                f"attempt {attempt}: {_summarize_exception_chain(exc)}"
            )

            LOGGER.warning(
                "Task %s failed on attempt %s/%s.\n%s",
                task_id,
                attempt,
                MAX_EXECUTION_ATTEMPTS,
                previous_traceback,
            )

            if attempt >= MAX_EXECUTION_ATTEMPTS:
                raise ExecutorError(
                    f"Task {task_id} failed after {MAX_EXECUTION_ATTEMPTS} attempts. "
                    f"Attempt summary: {'; '.join(attempt_failure_summaries)}"
                ) from last_exception

            attempt += 1
        finally:
            plt.close("all")

    raise ExecutorError(f"Task {task_id} failed after exhausting all retries.")


def run(
    datasets: dict[str, pd.DataFrame],
    task_plan: dict[str, Any],
    output_dir: str | Path,
) -> ExecutionResult:
    """Convenience entry point for the executor node."""
    return execute_task(datasets=datasets, task_plan=task_plan, output_dir=output_dir)


if __name__ == "__main__":
    dummy_datasets = {
        "labor.csv": pd.DataFrame({"age": [20, 30, 40], "province": ["天津", "北京", "天津"]}),
        "income.xlsx": pd.DataFrame({"age": [20, 30, 40], "wage": [50, 60, 70]}),
    }
    mock_task_plan = {
        "task_id": 1,
        "question_zh": "工资与年龄之间是否存在变化关系？",
        "analysis_type": "trend",
        "required_datasets": ["labor.csv", "income.xlsx"],
        "x_axis_col": "age",
        "y_axis_col": "wage",
        "x_axis_label_zh": "年龄",
        "y_axis_label_zh": "工资",
    }
    outputs_path = (Path(__file__).resolve().parents[1] / "outputs").resolve()
    os.makedirs(outputs_path, exist_ok=True)

    try:
        result = execute_task(datasets=dummy_datasets, task_plan=mock_task_plan, output_dir=outputs_path)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception:
        LOGGER.exception("Executor self-test failed.")
