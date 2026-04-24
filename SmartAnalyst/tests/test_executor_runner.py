from __future__ import annotations

import json

import pandas as pd
import pytest

from src.node3_executor import (
    ExecutorError,
    execute_task,
    _build_repair_prompt,
    _build_safe_debug_snapshot,
    _execute_generated_code_subprocess,
    _normalize_generated_code,
    _validate_generated_code,
)


VALID_CODE = """
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False
df = datasets['sample.csv'].copy()
# === PREP START ===
column_rename_map = {}
df_clean = df.copy()
data_plot = df_clean.copy()
print(df.shape)
print(df.info())
print(df.head())
# === PREP END ===
# === PLOT START ===
plt.figure()
plt.plot(data_plot['x'], data_plot['y'])
analysis_result_text = 'ok'
cleaning_summary_text = 'ok'
problem_solution_text = 'ok'
reflection_hint_text = 'ok'
plt.savefig(output_image_path, dpi=300, bbox_inches='tight')
# === PLOT END ===
""".strip()


def _build_code(plot_body: str, save_line: str = "plt.savefig(output_image_path, dpi=300, bbox_inches='tight')") -> str:
    return f"""
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False
df = datasets['sample.csv'].copy()
# === PREP START ===
column_rename_map = {{}}
df_clean = df.copy()
data_plot = df_clean.copy()
print(df.shape)
print(df.info())
print(df.head())
# === PREP END ===
# === PLOT START ===
{plot_body}
analysis_result_text = 'ok'
cleaning_summary_text = 'ok'
problem_solution_text = 'ok'
reflection_hint_text = 'ok'
{save_line}
# === PLOT END ===
""".strip()


def _run_generated_code(tmp_path, code_str: str) -> dict:
    return _execute_generated_code_subprocess(
        code_str=code_str,
        datasets={"sample.csv": pd.DataFrame({"x": [1, 2, 3], "y": [2, 4, 6], "z": [3, 2, 1]})},
        task_plan={
            "task_id": 1,
            "question_zh": "test",
            "analysis_type": "trend",
            "required_datasets": ["sample.csv"],
            "x_axis_col": "x",
            "y_axis_col": "y",
            "x_axis_label_zh": "x",
            "y_axis_label_zh": "y",
        },
        output_image_path=str(tmp_path / "chart.png"),
    )


def _task_plan() -> dict:
    return {
        "task_id": 1,
        "question_zh": "test",
        "analysis_type": "trend",
        "required_datasets": ["sample.csv"],
        "x_axis_col": "x",
        "y_axis_col": "y",
        "x_axis_label_zh": "x",
        "y_axis_label_zh": "y",
    }


def _datasets() -> dict[str, pd.DataFrame]:
    return {"sample.csv": pd.DataFrame({"x": [1, 2, 3], "y": [2, 4, 6], "z": [3, 2, 1]})}


@pytest.mark.parametrize(
    "bad_code",
    [
        "import os\n" + VALID_CODE,
        VALID_CODE + "\nopen('x.txt', 'w')",
        VALID_CODE + "\neval('1 + 1')",
        VALID_CODE + "\nos.environ",
    ],
)
def test_validate_generated_code_rejects_unsafe_patterns(bad_code):
    with pytest.raises(ExecutorError):
        _validate_generated_code(bad_code)


def test_validate_generated_code_accepts_code_without_imports():
    _validate_generated_code(VALID_CODE)


def test_validate_generated_code_reports_import_line_number():
    bad_code = "plt.rcParams['axes.unicode_minus'] = False\nimport os\n" + VALID_CODE

    with pytest.raises(ExecutorError, match=r"Disallowed import on line 2: import os"):
        _validate_generated_code(bad_code)


def test_normalize_generated_code_removes_duplicate_safe_imports():
    code = "\n".join(
        [
            VALID_CODE,
            "import pandas as pd",
            "import numpy as np",
            "import matplotlib.pyplot as plt",
            "from matplotlib import pyplot as plt",
            "import math",
        ]
    )

    normalized_code = _normalize_generated_code(code)

    assert "import pandas as pd" not in normalized_code
    assert "import numpy as np" not in normalized_code
    assert "import matplotlib.pyplot as plt" not in normalized_code
    assert "from matplotlib import pyplot as plt" not in normalized_code
    assert "import math" not in normalized_code
    _validate_generated_code(normalized_code)


def test_normalize_generated_code_keeps_dangerous_import_for_rejection():
    normalized_code = _normalize_generated_code("import subprocess\n" + VALID_CODE)

    assert "import subprocess" in normalized_code
    with pytest.raises(ExecutorError, match="Disallowed import on line 1: import subprocess"):
        _validate_generated_code(normalized_code)


def test_execute_task_removes_redundant_pandas_import_without_repair(tmp_path, monkeypatch):
    calls = []

    def fake_llm_caller(*, prompt, system_prompt):
        calls.append(prompt)
        return "import pandas as pd\n" + VALID_CODE

    monkeypatch.setattr("src.node3_executor.llm_caller", fake_llm_caller)

    result = execute_task(datasets=_datasets(), task_plan=_task_plan(), output_dir=tmp_path)

    assert len(calls) == 1
    assert "import pandas as pd" not in result["code_snippet"]
    assert result["analysis_text"] == "ok"


def test_execute_task_removes_redundant_from_matplotlib_import_without_repair(tmp_path, monkeypatch):
    calls = []

    def fake_llm_caller(*, prompt, system_prompt):
        calls.append(prompt)
        return "from matplotlib import pyplot as plt\n" + VALID_CODE

    monkeypatch.setattr("src.node3_executor.llm_caller", fake_llm_caller)

    result = execute_task(datasets=_datasets(), task_plan=_task_plan(), output_dir=tmp_path)

    assert len(calls) == 1
    assert "from matplotlib import pyplot as plt" not in result["code_snippet"]
    assert result["analysis_text"] == "ok"


def test_execute_task_repairs_disallowed_import_and_returns_import_free_code(tmp_path, monkeypatch):
    responses = iter(["import os\n" + VALID_CODE, VALID_CODE])
    prompts = []

    def fake_llm_caller(*, prompt, system_prompt):
        prompts.append(prompt)
        return next(responses)

    monkeypatch.setattr("src.node3_executor.llm_caller", fake_llm_caller)

    result = execute_task(datasets=_datasets(), task_plan=_task_plan(), output_dir=tmp_path)

    assert len(prompts) == 2
    assert "Disallowed import on line 1: import os" in prompts[1]
    assert "import " not in result["code_snippet"]
    assert "from " not in result["code_snippet"]
    assert result["analysis_text"] == "ok"


def test_repair_prompt_contains_data_empty_specific_instructions(tmp_path):
    prompt = _build_repair_prompt(
        datasets=_datasets(),
        task_plan=_task_plan(),
        output_image_path=str(tmp_path / "chart.png"),
        error_traceback="DataEmptyError: 数据清洗后数据量为0，无法绘图。请检查数据过滤条件。",
        previous_code="data_plot = df[df['x'].astype(str).str.contains('missing')]",
    )

    assert "DataEmptyError" in prompt
    assert "Remove or relax" in prompt
    assert "`str.contains`" in prompt
    assert "aggressive `dropna`" in prompt
    assert "`pd.to_numeric(..., errors='coerce')`" in prompt
    assert "放宽或移除导致空数据的筛选条件" in prompt
    assert "退回到更宽松的数据选择或原始字段绘图" in prompt


def test_safe_debug_snapshot_excludes_raw_rows():
    dataframe = pd.DataFrame(
        {
            "customer_name": ["secret-customer-a", "secret-customer-b"],
            "region": ["north-private", "south-private"],
            "amount": ["10", "bad"],
        }
    )

    snapshot = _build_safe_debug_snapshot(
        {
            "df": dataframe,
            "df_clean": dataframe.copy(),
            "data_plot": dataframe.iloc[0:0],
            "x_data": [],
        },
        empty_target="data_plot",
    )
    snapshot_text = json.dumps(snapshot, ensure_ascii=False)

    assert snapshot["frames"]["df"]["shape"] == [2, 3]
    assert snapshot["frames"]["data_plot"]["shape"] == [0, 3]
    assert "customer_name" in snapshot_text
    assert "non_null_count" in snapshot_text
    assert "numeric_column_candidates" in snapshot_text
    assert "secret-customer-a" not in snapshot_text
    assert "north-private" not in snapshot_text
    assert "bad" not in snapshot_text


def test_data_empty_error_message_uses_safe_snapshot_without_rows(tmp_path):
    code = _build_code(
        """
data_plot = data_plot[data_plot['customer_name'].astype(str).str.contains('missing')]
plt.figure()
plt.plot(data_plot['x'], data_plot['y'])
""".strip()
    )

    with pytest.raises(ExecutorError) as exc_info:
        _execute_generated_code_subprocess(
            code_str=code,
            datasets={
                "sample.csv": pd.DataFrame(
                    {
                        "x": [1, 2, 3],
                        "y": [2, 4, 6],
                        "customer_name": ["secret-customer-a", "secret-customer-b", "secret-customer-c"],
                    }
                )
            },
            task_plan=_task_plan(),
            output_image_path=str(tmp_path / "chart.png"),
        )

    error_text = str(exc_info.value)
    assert "Safe debug snapshot" in error_text
    assert "customer_name" in error_text
    assert "non_null_count" in error_text
    assert "secret-customer-a" not in error_text


def test_execute_task_repairs_data_empty_error(tmp_path, monkeypatch):
    empty_code = _build_code(
        """
data_plot = data_plot[data_plot['x'].astype(str).str.contains('missing')]
plt.figure()
plt.plot(data_plot['x'], data_plot['y'])
""".strip()
    )
    responses = iter([empty_code, VALID_CODE])
    prompts = []

    def fake_llm_caller(*, prompt, system_prompt):
        prompts.append(prompt)
        return next(responses)

    monkeypatch.setattr("src.node3_executor.llm_caller", fake_llm_caller)

    result = execute_task(datasets=_datasets(), task_plan=_task_plan(), output_dir=tmp_path)

    assert len(prompts) == 2
    assert "DataEmptyError" in prompts[1]
    assert "放宽或移除导致空数据的筛选条件" in prompts[1]
    assert result["analysis_text"] == "ok"


def test_subprocess_runner_executes_generated_code(tmp_path):
    output_image_path = str(tmp_path / "chart.png")
    outputs = _execute_generated_code_subprocess(
        code_str=VALID_CODE,
        datasets={"sample.csv": pd.DataFrame({"x": [1, 2, 3], "y": [2, 4, 6]})},
        task_plan={
            "task_id": 1,
            "question_zh": "test",
            "analysis_type": "trend",
            "required_datasets": ["sample.csv"],
            "x_axis_col": "x",
            "y_axis_col": "y",
            "x_axis_label_zh": "x",
            "y_axis_label_zh": "y",
        },
        output_image_path=output_image_path,
    )

    assert outputs["analysis_result_text"] == "ok"
    assert outputs["column_rename_map"] == {}
    assert (tmp_path / "chart.png").exists()


def test_single_main_axes_passes(tmp_path):
    code = _build_code(
        """
plt.figure()
plt.plot(data_plot['x'], data_plot['y'])
""".strip()
    )

    outputs = _run_generated_code(tmp_path, code)

    assert outputs["analysis_result_text"] == "ok"
    assert (tmp_path / "chart.png").exists()


def test_horizontal_subplots_are_rejected(tmp_path):
    code = _build_code(
        """
fig, axes = plt.subplots(1, 2)
axes[0].plot(data_plot['x'], data_plot['y'])
axes[1].plot(data_plot['x'], data_plot['z'])
""".strip()
    )

    with pytest.raises(ExecutorError, match="multiple independent chart panels"):
        _run_generated_code(tmp_path, code)


def test_vertical_subplots_are_rejected(tmp_path):
    code = _build_code(
        """
fig, axes = plt.subplots(2, 1)
axes[0].plot(data_plot['x'], data_plot['y'])
axes[1].plot(data_plot['x'], data_plot['z'])
""".strip()
    )

    with pytest.raises(ExecutorError, match="multiple independent chart panels"):
        _run_generated_code(tmp_path, code)


def test_main_axes_with_colorbar_passes(tmp_path):
    code = _build_code(
        """
plt.figure()
points = plt.scatter(data_plot['x'], data_plot['y'], c=data_plot['z'])
plt.colorbar(points)
""".strip()
    )

    outputs = _run_generated_code(tmp_path, code)

    assert outputs["analysis_result_text"] == "ok"
    assert (tmp_path / "chart.png").exists()


def test_main_axes_with_twinx_passes(tmp_path):
    code = _build_code(
        """
fig, ax = plt.subplots()
ax.plot(data_plot['x'], data_plot['y'])
ax2 = ax.twinx()
ax2.plot(data_plot['x'], data_plot['z'])
""".strip()
    )

    outputs = _run_generated_code(tmp_path, code)

    assert outputs["analysis_result_text"] == "ok"
    assert (tmp_path / "chart.png").exists()


def test_multiple_savefig_calls_are_rejected(tmp_path):
    code = _build_code(
        """
plt.figure()
plt.plot(data_plot['x'], data_plot['y'])
plt.savefig(output_image_path, dpi=300, bbox_inches='tight')
""".strip()
    )

    with pytest.raises(ExecutorError, match="Multiple savefig calls"):
        _run_generated_code(tmp_path, code)


def test_savefig_must_target_output_image_path(tmp_path):
    code = _build_code(
        """
plt.figure()
plt.plot(data_plot['x'], data_plot['y'])
""".strip(),
        save_line="plt.savefig(output_image_path + '.extra.png', dpi=300, bbox_inches='tight')",
    )

    with pytest.raises(ExecutorError, match="provided output_image_path"):
        _run_generated_code(tmp_path, code)
