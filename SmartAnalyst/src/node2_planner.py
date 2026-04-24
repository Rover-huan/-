"""Node 2 planner for generating a multi-dataset academic title and research tasks."""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Literal, TypedDict

if __package__ in {None, ""}:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from src.node1_scanner import llm_caller


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger().setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)

PLANNED_TASK_COUNT = 10

SYSTEM_PROMPT = f"""
你是一名资深的数据分析研究设计专家。你现在拥有多个数据集的结构信息。
你的任务是先生成一个正式、学术化、大学课程作业风格的全局报告标题，
然后规划 {PLANNED_TASK_COUNT} 个具有学术深度、可执行、可视化的分析任务。

多表规划要求：
1. 你可以选择对某一个核心数据集进行深挖，但更鼓励你寻找不同数据集之间的共同键，
   例如“年份”“省份”“城市”“行业”“月份”等，并规划跨表联合分析。
2. 如果任务适合跨表，请明确设想通过 `pd.merge` 完成数据对齐，并在 `required_datasets`
   中列出相关文件名。
3. 不要编造不存在的公共键、字段名或文件名，必须严格依据提供的 dataset metadata 规划。

输出要求：
1. 你必须只输出原始 JSON 对象，不要输出 markdown，不要输出解释，不要输出额外文本。
2. 所有说明性文本必须使用简体中文。
3. 最终 JSON 必须严格符合以下结构：
{{
  "report_title": "基于……的……研究报告",
  "tasks": [
    {{
      "task_id": 1,
      "question_zh": "中文研究问题",
      "analysis_type": "correlation|distribution|trend",
      "required_datasets": ["file1.csv", "file2.xlsx"],
      "x_axis_col": "原始列名",
      "y_axis_col": "原始列名",
      "x_axis_label_zh": "中文横轴名称",
      "y_axis_label_zh": "中文纵轴名称"
    }}
  ]
}}

严格约束：
1. tasks 必须恰好包含 {PLANNED_TASK_COUNT} 个任务。
2. task_id 必须按 1 到 {PLANNED_TASK_COUNT} 递增。
3. analysis_type 只能是 correlation、distribution、trend 之一。
4. required_datasets 必须是非空数组，且每个文件名都必须来自提供的数据集列表。
5. x_axis_col 和 y_axis_col 必须直接来自 required_datasets 对应数据集中的真实列名，不能虚构。
6. {PLANNED_TASK_COUNT} 个任务必须在分析角度上有明显差异，并尽量包含部分跨表联合分析任务。
7. 每个任务都必须适合生成一张候选图，方便用户后续从候选图中进行筛选。
8. report_title 必须正式、学术化、适合作为大学课程作业题目。
""".strip()

USER_PROMPT_TEMPLATE = f"""
以下是多数据集的元信息：
{{dataset_meta_json}}

请根据这些数据集结构信息：
1. 先生成一个正式、学术化、简体中文的全局报告标题。
2. 再规划 {PLANNED_TASK_COUNT} 个研究任务。
3. 每个任务都必须包含 `required_datasets` 字段，明确指出该任务需要使用哪些文件。
4. 如果存在明显的公共键或可比维度，请优先考虑规划跨表联合分析任务。

再次强调：
1. 只能输出一个 JSON 对象。
2. JSON 只能包含两个顶层键：`report_title` 和 `tasks`。
3. 每个任务只能包含这些字段：
   `task_id`, `question_zh`, `analysis_type`, `required_datasets`,
   `x_axis_col`, `y_axis_col`, `x_axis_label_zh`, `y_axis_label_zh`
4. tasks 数量必须恰好为 {PLANNED_TASK_COUNT}。
5. 每个任务都必须可视化、可生成候选图，并且与其他任务有实质差异。
""".strip()

JSON_FENCE_PATTERN = re.compile(
    r"^\s*```(?:json)?\s*(?P<json>[\s\S]*?)\s*```\s*$",
    flags=re.IGNORECASE,
)
ALLOWED_ANALYSIS_TYPES = {"correlation", "distribution", "trend"}
REQUIRED_PLAN_KEYS = {
    "task_id",
    "question_zh",
    "analysis_type",
    "required_datasets",
    "x_axis_col",
    "y_axis_col",
    "x_axis_label_zh",
    "y_axis_label_zh",
}
REQUIRED_OUTPUT_KEYS = {"report_title", "tasks"}


class ResearchPlan(TypedDict):
    """One structured analytical research plan."""

    task_id: int
    question_zh: str
    analysis_type: Literal["correlation", "distribution", "trend"]
    required_datasets: list[str]
    x_axis_col: str
    y_axis_col: str
    x_axis_label_zh: str
    y_axis_label_zh: str


class PlannerOutput(TypedDict):
    """Academic title plus a fixed number of structured analytical research plans."""

    report_title: str
    tasks: list[ResearchPlan]


class PlannerParseError(ValueError):
    """Raised when the planner response cannot be parsed as JSON."""


class PlannerValidationError(ValueError):
    """Raised when the planner response does not match the expected schema."""


def clean_json_response(raw_text: str) -> str:
    """Strip markdown JSON fences and surrounding whitespace from an LLM response."""
    cleaned = raw_text.strip()
    fenced_match = JSON_FENCE_PATTERN.match(cleaned)
    if fenced_match:
        cleaned = fenced_match.group("json").strip()
    return cleaned


def _build_user_prompt(dataset_meta: dict[str, Any]) -> str:
    """Format dataset metadata into the user prompt."""
    dataset_meta_json = json.dumps(dataset_meta, ensure_ascii=False, indent=2)
    return USER_PROMPT_TEMPLATE.format(dataset_meta_json=dataset_meta_json)


def _validate_report_title(report_title: Any) -> str:
    """Validate the global academic report title."""
    if not isinstance(report_title, str) or not report_title.strip():
        raise PlannerValidationError("report_title must be a non-empty string.")

    normalized_title = report_title.strip()
    if len(normalized_title) < 8:
        raise PlannerValidationError("report_title is too short to sound like an academic title.")

    return normalized_title


def _build_dataset_column_map(dataset_meta: dict[str, Any]) -> dict[str, set[str]]:
    """Build a fast lookup table of dataset name -> available columns."""
    datasets_meta = dataset_meta.get("datasets")
    if not isinstance(datasets_meta, list):
        return {}

    dataset_column_map: dict[str, set[str]] = {}
    for item in datasets_meta:
        if not isinstance(item, dict):
            continue
        dataset_name = str(item.get("dataset_name", "")).strip()
        raw_columns = item.get("columns", [])
        if not dataset_name or not isinstance(raw_columns, list):
            continue
        dataset_column_map[dataset_name] = {
            str(column).strip()
            for column in raw_columns
            if str(column).strip()
        }
    return dataset_column_map


def _validate_required_datasets(
    required_datasets: Any,
    index: int,
    available_dataset_names: set[str],
) -> list[str]:
    """Validate the required_datasets field for one task."""
    if not isinstance(required_datasets, list) or not required_datasets:
        raise PlannerValidationError(
            f"Plan item at index {index} must contain a non-empty required_datasets list."
        )

    normalized_required: list[str] = []
    seen: set[str] = set()
    for item in required_datasets:
        dataset_name = str(item).strip()
        if not dataset_name:
            continue
        if dataset_name not in available_dataset_names:
            raise PlannerValidationError(
                f"required_datasets contains unknown dataset {dataset_name!r} at index {index}."
            )
        if dataset_name not in seen:
            seen.add(dataset_name)
            normalized_required.append(dataset_name)

    if not normalized_required:
        raise PlannerValidationError(
            f"Plan item at index {index} must contain at least one valid dataset name."
        )
    return normalized_required


def _validate_plan_item(
    item: Any,
    index: int,
    dataset_column_map: dict[str, set[str]],
    available_dataset_names: set[str],
) -> ResearchPlan:
    """Validate a single planner item against the required schema."""
    if not isinstance(item, dict):
        raise PlannerValidationError(f"Plan item at index {index} must be a dictionary.")

    item_keys = set(item.keys())
    if item_keys != REQUIRED_PLAN_KEYS:
        raise PlannerValidationError(
            f"Plan item at index {index} must contain exactly these keys: "
            f"{sorted(REQUIRED_PLAN_KEYS)}."
        )

    task_id = item["task_id"]
    if not isinstance(task_id, int) or task_id != index + 1:
        raise PlannerValidationError(
            f"Plan item at index {index} must have task_id={index + 1}."
        )

    analysis_type = item["analysis_type"]
    if analysis_type not in ALLOWED_ANALYSIS_TYPES:
        raise PlannerValidationError(
            f"Plan item at index {index} has invalid analysis_type: {analysis_type!r}."
        )

    for field_name in (
        "question_zh",
        "x_axis_col",
        "y_axis_col",
        "x_axis_label_zh",
        "y_axis_label_zh",
    ):
        field_value = item[field_name]
        if not isinstance(field_value, str) or not field_value.strip():
            raise PlannerValidationError(
                f"Field '{field_name}' in plan item at index {index} must be a non-empty string."
            )

    normalized_required_datasets = _validate_required_datasets(
        required_datasets=item["required_datasets"],
        index=index,
        available_dataset_names=available_dataset_names,
    )

    available_columns: set[str] = set()
    for dataset_name in normalized_required_datasets:
        available_columns.update(dataset_column_map.get(dataset_name, set()))

    if available_columns:
        if item["x_axis_col"] not in available_columns:
            raise PlannerValidationError(
                f"x_axis_col {item['x_axis_col']!r} is not present in required_datasets columns."
            )
        if item["y_axis_col"] not in available_columns:
            raise PlannerValidationError(
                f"y_axis_col {item['y_axis_col']!r} is not present in required_datasets columns."
            )

    return {
        "task_id": task_id,
        "question_zh": item["question_zh"].strip(),
        "analysis_type": analysis_type,
        "required_datasets": normalized_required_datasets,
        "x_axis_col": item["x_axis_col"].strip(),
        "y_axis_col": item["y_axis_col"].strip(),
        "x_axis_label_zh": item["x_axis_label_zh"].strip(),
        "y_axis_label_zh": item["y_axis_label_zh"].strip(),
    }


def parse_research_plans(
    raw_response: str,
    dataset_meta: dict[str, Any],
) -> PlannerOutput:
    """Clean, parse, and validate the planner response."""
    cleaned_response = clean_json_response(raw_response)

    try:
        parsed = json.loads(cleaned_response)
    except json.JSONDecodeError as exc:
        LOGGER.error("Failed to parse planner response as JSON. Raw response: %s", raw_response)
        raise PlannerParseError("Planner returned invalid JSON.") from exc

    if not isinstance(parsed, dict):
        raise PlannerValidationError("Planner response must be a JSON object.")

    parsed_keys = set(parsed.keys())
    if parsed_keys != REQUIRED_OUTPUT_KEYS:
        raise PlannerValidationError(
            "Planner response must contain exactly these keys: "
            f"{sorted(REQUIRED_OUTPUT_KEYS)}."
        )

    tasks = parsed.get("tasks")
    if not isinstance(tasks, list):
        raise PlannerValidationError("Planner response field 'tasks' must be a JSON array.")

    if len(tasks) != PLANNED_TASK_COUNT:
        raise PlannerValidationError(
            f"Planner response must contain exactly {PLANNED_TASK_COUNT} tasks, got {len(tasks)}."
        )

    dataset_column_map = _build_dataset_column_map(dataset_meta)
    available_dataset_names = set(dataset_column_map.keys())
    validated_plans = [
        _validate_plan_item(
            item=item,
            index=index,
            dataset_column_map=dataset_column_map,
            available_dataset_names=available_dataset_names,
        )
        for index, item in enumerate(tasks)
    ]

    return {
        "report_title": _validate_report_title(parsed.get("report_title")),
        "tasks": validated_plans,
    }


def plan_research(dataset_meta: dict[str, Any]) -> PlannerOutput:
    """Generate a global academic title and a fixed number of structured research plans."""
    if not isinstance(dataset_meta, dict):
        raise ValueError("dataset_meta must be a dictionary.")

    LOGGER.info("Starting research planning with multi-dataset metadata.")
    user_prompt = _build_user_prompt(dataset_meta)
    raw_response = llm_caller(prompt=user_prompt, system_prompt=SYSTEM_PROMPT)

    try:
        plan_bundle = parse_research_plans(raw_response=raw_response, dataset_meta=dataset_meta)
    except (PlannerParseError, PlannerValidationError):
        LOGGER.exception("Planner response parsing or validation failed.")
        raise

    LOGGER.info(
        "Successfully generated report title and %s research plans.",
        len(plan_bundle["tasks"]),
    )
    return plan_bundle


def run(dataset_meta: dict[str, Any]) -> PlannerOutput:
    """Convenience entry point for the planner node."""
    return plan_research(dataset_meta)


if __name__ == "__main__":
    mock_dataset_meta = {
        "dataset_count": 2,
        "dataset_names": ["labor.csv", "province.xlsx"],
        "combined_summary_text": "数据集 labor.csv 与 province.xlsx 均包含年份和省份维度。",
        "datasets": [
            {
                "dataset_name": "labor.csv",
                "dataset_path": "labor.csv",
                "columns": ["年份", "工资", "省份"],
                "sample_data": [
                    {"年份": 2021, "工资": 6800, "省份": "天津"},
                    {"年份": 2022, "工资": 7200, "省份": "天津"},
                ],
                "file_type": "csv",
                "total_columns": 3,
            },
            {
                "dataset_name": "province.xlsx",
                "dataset_path": "province.xlsx",
                "columns": ["年份", "省份", "产业结构"],
                "sample_data": [
                    {"年份": 2021, "省份": "天津", "产业结构": "服务业主导"},
                    {"年份": 2022, "省份": "天津", "产业结构": "服务业主导"},
                ],
                "file_type": "excel",
                "total_columns": 3,
            },
        ],
    }

    try:
        result = plan_research(mock_dataset_meta)
        print("报告标题：")
        print(result["report_title"])
        print("\n研究任务：")
        print(json.dumps(result["tasks"], ensure_ascii=False, indent=2))
    except Exception:
        LOGGER.exception("Planner self-test failed.")
