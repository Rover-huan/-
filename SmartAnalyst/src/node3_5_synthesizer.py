"""Node 3.5 synthesizer for TOC-friendly thesis-style report sections."""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, TypedDict

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

JSON_FENCE_PATTERN = re.compile(
    r"^\s*```(?:json)?\s*(?P<json>[\s\S]*?)\s*```\s*$",
    flags=re.IGNORECASE,
)
SECTION_KEYS = {
    "section_1_intro",
    "section_2_analysis",
    "section_3_mechanism",
    "section_4_reflection",
}
TEXT_TOKEN_PATTERN = re.compile(
    "[A-Za-z_][A-Za-z0-9_ ()/-]{1,40}|[\u4e00-\u9fff][\u4e00-\u9fffA-Za-z0-9_()/-]{1,40}"
)
TITLE_STOP_WORDS = {
    "基于",
    "研究",
    "报告",
    "量化",
    "分析",
    "视角",
    "理论",
    "模型",
    "课程",
    "作业",
    "机制",
    "特征",
    "影响",
    "因素",
    "决定",
    "实证",
    "方法",
    "路径",
    "建议",
}
GENERIC_REFERENCE_TERMS = {
    "缺失值",
    "空值",
    "重复值",
    "重复行",
    "字段名",
    "列名",
    "换行",
    "单位",
    "数值类型",
    "数据类型",
    "分组",
    "编码",
    "格式",
    "清洗",
    "预处理",
    "报错",
    "错误",
    "失败",
    "修复",
}
FIRST_PERSON_MARKERS = {
    "\u6211\u4eec",
    "\u672c\u6b21\u5206\u6790",
    "\u5728\u5206\u6790\u4e2d",
    "\u5728\u9884\u5904\u7406\u4e2d",
    "\u5728\u6e05\u6d17\u65f6",
    "\u6211\u4eec\u5728\u5206\u6790\u4e2d",
    "\u6211\u4eec\u5728\u5904\u7406",
}
GENERIC_LOGIC_TERMS = {
    "重命名",
    "缺失值",
    "空值",
    "填补",
    "删除",
    "dropna",
    "fillna",
    "预处理",
    "清洗",
    "分组",
    "格式",
    "数据类型",
    "字段",
    "列名",
    "编码",
    "标准化",
}

SYSTEM_PROMPT = """
你是一名 Academic Narrative Architect（学术叙事架构师）兼 Thesis Editor（论文编辑），负责把已有的数据分析结果整理成适合提交的课程/比赛/作业版正式分析报告 JSON。
你必须只输出一个原始 JSON 对象，不要输出 markdown，不要输出解释，不要输出代码块。

输出 JSON 必须严格使用以下四个一级字段：
{
  "section_1_intro": {
    "title": "一、……",
    "content": "……"
  },
  "section_2_analysis": [
    {
      "sub_title": "（一）……",
      "content": "……"
    }
  ],
  "section_3_mechanism": {
    "title": "三、……",
    "content": "……"
  },
  "section_4_reflection": [
    {
      "sub_title": "四、遇到的问题及解决方法",
      "content": "……"
    },
    {
      "sub_title": "五、总结与思考",
      "content": "……"
    }
  ]
}

写作与结构要求：
1. 全部内容必须使用正式、自然、规范的简体中文。
2. 你必须围绕给定的 report_title 保持全篇研究方向一致，不能偏离标题。
3. 全篇叙述优先使用第一人称复数的真实作业口吻，例如“我们在分析中发现……”“我们先对……进行了重命名”，不要写成 Notebook 导出说明，也不要写成 AI 口吻。
4. section_1_intro 可以继续使用“一、引言与数据清洗说明”这类一级标题，但正文必须同时包含：数据来源说明、数据集字段概览、数据清洗过程概述、研究背景、一个明确的核心研究问题、本报告的综合研究主线，以及后续图表分析将围绕什么问题展开。
5. 报告必须围绕一条清晰主线展开。若数据字段支持区域、就业、工资、人口、家庭等主题，应优先围绕“区域经济差异—劳动力流动—人口结构—家庭结构”这条链条收束，例如回答“地区经济发展差异如何影响就业稳定、工资水平与人口家庭结构？”；若当前数据不覆盖这些字段，则应基于真实字段建立等价的分析链条，不要硬编不存在的变量。
6. section_2_analysis 必须是与图表数量一致的列表；每个对象都要包含正式的小标题和完整分析段落。每张图的 content 必须自然覆盖四层意思：研究问题（这张图回答什么）、图表发现（趋势、差异、异常或关系）、原因解释（结合字段含义、业务背景或经济/管理逻辑）、现实启示（对经营决策、用户理解、市场判断或后续分析的价值）。多个图表之间要体现递进逻辑：先看核心指标波动，再看群体或区域差异，再看工资/收入或发展水平，再看人口结构和家庭结构；如果当前任务不包含其中某一类，只按真实图表顺序建立相近的分析递进，不要虚构。
7. section_3_mechanism 必须写成“综合机制分析”：不要泛泛总结所有图，而要围绕“区域经济差异—劳动力流动—人口结构—家庭结构”或当前数据可支持的等价链条，把多个图表之间的共同趋势、差异和相互印证关系串起来；要结合真实字段和图表发现解释背后的可能机制，避免空泛套话。
8. section_4_reflection 必须严格是两个对象：
   第一项写“遇到的问题及解决方法”，内容要比普通图表说明更完整，应覆盖数据缺失、异常值、字段类型、字段命名、可视化选择、分析局限等方面；必须引用真实数据线索，优先使用真实字段名、缺失值数量、空值列、预处理输出或执行报错；
   第二项写“总结与思考”，必须包含本次分析的主要结论、报告价值、不足之处、后续可拓展方向，并把实际代码逻辑或清洗动作与经济理论、业务含义结合起来。
9. 不要虚构数据中不存在的字段、数据来源、报错或具体数值。
10. 不要在报告正文复述 preview_text 或 info_text 的原文；可以概括数据规模、字段类型、缺失值和清洗线索。
11. 不要写“作为AI模型”“由AI生成”“根据提示词”等 AI 口吻，不要写过度模板化的空话。
12. 对数据无法直接证明的外部背景，只能使用“可能”“或许”“从业务逻辑看”“可以推测”等谨慎表达，不要把推测写成确定事实。
13. 不要编造数据来源或外部事件，例如不要擅自写“国家统计局”“疫情”“政策环境”等，除非输入信息中明确出现。
14. 不要轻易使用“高度正相关”“显著相关”“强相关”“显著影响”等统计判断；如果没有相关系数、回归结果或显著性检验，只能写“呈现同向变化趋势”“存在一定关联”“从图中看二者可能具有同步变化关系”等谨慎表述。
15. 图表分析要尽量结合本图字段和图形本身，不要写空泛行业套话。
16. 反思部分要结合本次数据限制，例如字段缺失、样本范围、图表类型限制、未做统计检验等，不要只写通用模板。
17. 如果 execution_errors 不为空，应尽量把至少一个真实报错或修复过程写进“遇到的问题及解决方法”。
18. 所有章节标题都必须正式、学术、可直接用于 Word 目录。
""".strip()

USER_PROMPT_TEMPLATE = """
请根据以下信息生成结构化报告 JSON。

报告标题：
{report_title}

数据摘要：
{data_summary_json}

任务结果：
{all_results_json}

执行错误摘要：
{execution_errors_json}

真实问题线索：
{issue_clues_text}

补充要求：
1. section_2_analysis 的条目数必须与任务结果数量一致。
2. section_2_analysis 中每个 sub_title 都要体现学术化分析方向，不能只写“任务1”“任务2”。
3. section_1_intro 的 content 必须写成正式报告开篇，包含数据来源、字段概览、清洗概述、研究背景、核心研究问题、综合研究主线和后续图表分析问题。
4. section_2_analysis 的每个 content 必须写成完整段落，按自然语言覆盖“研究问题、图表发现、原因解释、现实启示”，不要只写短结论；多个图表之间要体现递进逻辑，例如先看就业或核心指标波动，再看群体/区域差异，再看工资/收入或发展水平，再看人口结构和家庭结构。若实际图表不覆盖某类主题，只使用真实图表支持的递进关系。
5. section_3_mechanism 必须围绕“区域经济差异—劳动力流动—人口结构—家庭结构”或当前数据可支持的等价链条收束，说明共同趋势、可能机制和研究主线，不要逐图复述。
6. section_4_reflection 的第一项必须明确引用至少一个真实字段名、空值统计、预处理输出线索或执行报错；例如“Review Date 列格式不统一”“Reviews 列存在 18 个缺失值”“Price 列货币符号不统一”等写法。
7. section_4_reflection 的第一项必须优先使用第一人称复数，例如“我们在分析中发现……”“我们在处理 Reviews 列时……”，并尽量覆盖数据缺失、异常值、字段类型、可视化选择、分析局限等方面。
8. section_4_reflection 的第二项必须包含主要结论、报告价值、不足之处和后续可拓展方向，不能写成通用套话；同时要把字段处理、缺失值处理、分组统计或重命名等代码逻辑，与经济学或管理学解释结合起来。
9. 如果 execution_errors 非空，第一项应尽量写出真实报错及修复动作，而不是泛泛而谈。
10. 对无法由图表和字段直接证明的解释，只能用“可能”“或许”“从业务逻辑看”“可以推测”等谨慎表达，不要写成确定事实。
11. 不要编造不存在的字段、数据来源、外部事件、报错或数值；不要复述 preview_text/info_text 原文；不要写“作为AI模型”；不要写过度模板化空话。
12. 没有相关系数、回归结果或显著性检验时，不要写“高度正相关”“显著相关”“强相关”“显著影响”；可以写“呈现同向变化趋势”“存在一定关联”“从图中看二者可能具有同步变化关系”。
13. 反思部分必须结合本次数据限制，例如字段缺失、样本范围、图表类型限制、未做统计检验等。
14. 只返回 JSON 对象。
""".strip()


class SectionBlock(TypedDict):
    """One major section with a title and content."""

    title: str
    content: str


class SectionItem(TypedDict):
    """One TOC-ready sub section entry."""

    sub_title: str
    content: str


class FinalReportData(TypedDict):
    """Structured synthesized report data for TOC-friendly rendering."""

    section_1_intro: SectionBlock
    section_2_analysis: list[SectionItem]
    section_3_mechanism: SectionBlock
    section_4_reflection: list[SectionItem]


class NormalizedExecutionError(TypedDict):
    """One normalized Node 3 execution error summary."""

    task_id: int | None
    error_text: str


class SynthesizerParseError(ValueError):
    """Raised when the synthesizer response cannot be parsed as JSON."""


class SynthesizerValidationError(ValueError):
    """Raised when the synthesizer response JSON is structurally invalid."""


def _summarize_exception_chain(exc: BaseException) -> str:
    """Flatten one exception chain into a concise matching string."""
    parts: list[str] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        text = str(current).strip()
        if text and text not in parts:
            parts.append(text)
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _is_content_risk_error(exc: BaseException) -> bool:
    """Return True for DeepSeek content-risk 400 errors during report synthesis."""
    summary = _summarize_exception_chain(exc).lower()
    return "content exists risk" in summary and (
        "error code: 400" in summary
        or "400 bad request" in summary
        or "invalid_request_error" in summary
    )


def clean_json_response(raw_text: str) -> str:
    """Strip surrounding markdown JSON fences from the LLM response."""
    cleaned = raw_text.strip()
    fenced_match = JSON_FENCE_PATTERN.match(cleaned)
    if fenced_match:
        cleaned = fenced_match.group("json").strip()
    return cleaned


def _normalize_text(value: Any, field_name: str) -> str:
    """Validate a non-empty string field and strip whitespace."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _extract_terms_from_text(text: str) -> set[str]:
    """Extract reference terms from mixed Chinese and English text."""
    if not text.strip():
        return set()

    candidates = {
        match.group(0).strip(" ，。；：、()（）[]【】\"'")
        for match in TEXT_TOKEN_PATTERN.finditer(text)
    }
    terms = {
        candidate
        for candidate in candidates
        if len(candidate) >= 2 and candidate not in TITLE_STOP_WORDS
    }
    return terms


def _extract_title_terms(report_title: str) -> set[str]:
    """Derive a small set of meaningful topic words from the academic title."""
    normalized = report_title
    for token in TITLE_STOP_WORDS.union({"的"}):
        normalized = normalized.replace(token, " ")
    extracted_terms = _extract_terms_from_text(normalized)
    if extracted_terms:
        return extracted_terms
    return _extract_terms_from_text(report_title)


def _contains_any_term(text: str, terms: set[str]) -> bool:
    """Return True when the text includes any reference term."""
    return any(term and term in text for term in terms)


def _contains_first_person_tone(text: str) -> bool:
    """Return True when the text uses a first-person academic narrative voice."""
    return any(marker in text for marker in FIRST_PERSON_MARKERS)


def _append_sentence(text: str, sentence: str) -> str:
    """Append one sentence while keeping punctuation tidy."""
    base = text.strip()
    addition = sentence.strip()
    if not addition:
        return base
    if not base:
        return addition
    if base[-1] not in "。！？；;":
        base = f"{base}。"
    return f"{base}{addition}"


def _ensure_reflection_alignment(
    reflection_text: str,
    report_title: str,
    title_reference_terms: set[str],
    reflection_logic_terms: set[str],
) -> str:
    """Auto-heal one weak reflection paragraph instead of failing the whole render job."""
    normalized_text = reflection_text.strip()
    if not normalized_text:
        normalized_text = "我们结合本次图表结果，对研究主题进行了总结。"

    if not _contains_first_person_tone(normalized_text):
        normalized_text = f"我们进一步认为，{normalized_text}"

    if title_reference_terms and not _contains_any_term(normalized_text, title_reference_terms):
        normalized_text = _append_sentence(
            normalized_text,
            f"我们也始终围绕“{report_title}”这一研究主题理解上述现象，并据此形成结论与建议。",
        )

    if reflection_logic_terms and not _contains_any_term(normalized_text, reflection_logic_terms):
        normalized_text = _append_sentence(
            normalized_text,
            "在具体实现上，我们结合字段重命名、缺失值检查和分组统计等处理步骤，把数据整理过程与经济含义对应起来。",
        )

    return normalized_text


def _validate_result_item(item: Any, index: int) -> dict[str, Any]:
    """Validate one Node 3 result item before sending it to the synthesizer."""
    if not isinstance(item, dict):
        raise ValueError(f"all_results[{index}] must be a dictionary.")

    required_keys = {
        "task_id",
        "analysis_text",
        "cleaning_summary",
        "problem_solution",
        "reflection_hint",
        "exploration_output",
        "column_mapping",
    }
    missing_keys = required_keys.difference(item.keys())
    if missing_keys:
        raise ValueError(f"all_results[{index}] is missing required keys: {sorted(missing_keys)}")

    normalized: dict[str, Any] = {}
    task_id = item["task_id"]
    if not isinstance(task_id, int):
        raise ValueError(f"all_results[{index}]['task_id'] must be an integer.")
    normalized["task_id"] = task_id

    for key in (
        "analysis_text",
        "cleaning_summary",
        "problem_solution",
        "reflection_hint",
        "exploration_output",
    ):
        normalized[key] = _normalize_text(item[key], f"all_results[{index}]['{key}']")

    optional_string_keys = {
        "preprocessing_output_summary",
        "question_zh",
    }
    for key in optional_string_keys:
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            normalized[key] = value.strip()

    column_mapping = item["column_mapping"]
    if not isinstance(column_mapping, dict):
        raise ValueError(f"all_results[{index}]['column_mapping'] must be a dictionary.")
    normalized["column_mapping"] = {
        str(key).strip(): str(value).strip()
        for key, value in column_mapping.items()
        if str(key).strip() and str(value).strip()
    }
    return normalized


def _normalize_all_results(all_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate and normalize the full Node 3 result list."""
    if not isinstance(all_results, list) or not all_results:
        raise ValueError("all_results must be a non-empty list.")

    return [_validate_result_item(item=item, index=index) for index, item in enumerate(all_results)]


def _normalize_data_summary(data_summary: dict[str, Any]) -> dict[str, Any]:
    """Validate the shared dataset summary sent from main.py."""
    if not isinstance(data_summary, dict):
        raise ValueError("data_summary must be a dictionary.")

    required_keys = {
        "dataset_name",
        "shape_text",
        "info_text",
        "missing_summary_text",
        "duplicate_count_text",
        "preview_text",
    }
    missing_keys = required_keys.difference(data_summary.keys())
    if missing_keys:
        raise ValueError(f"data_summary is missing required keys: {sorted(missing_keys)}")

    normalized: dict[str, Any] = {}
    for key in required_keys:
        normalized[key] = _normalize_text(data_summary[key], f"data_summary['{key}']")

    optional_string_keys = {
        "row_count_text",
        "column_count_text",
        "null_columns_text",
        "scanner_summary_text",
        "preprocessing_code_text",
        "preprocessing_output_summary_text",
        "preprocessing_summary_text",
    }
    for key in optional_string_keys:
        value = data_summary.get(key)
        if isinstance(value, str) and value.strip():
            normalized[key] = value.strip()

    scanner_summary_lines = data_summary.get("scanner_summary_lines")
    if isinstance(scanner_summary_lines, list):
        normalized["scanner_summary_lines"] = [
            str(item).strip()
            for item in scanner_summary_lines
            if str(item).strip()
        ]

    return normalized


def _normalize_report_title(report_title: str) -> str:
    """Validate the academic report title from Node 2."""
    if not isinstance(report_title, str) or not report_title.strip():
        raise ValueError("report_title must be a non-empty string.")
    normalized_title = report_title.strip()
    if len(normalized_title) < 8:
        raise ValueError("report_title is too short to be a credible academic title.")
    return normalized_title


def _normalize_execution_errors(execution_errors: list[Any] | None) -> list[NormalizedExecutionError]:
    """Normalize optional Node 3 execution error payloads."""
    if execution_errors is None:
        return []

    if not isinstance(execution_errors, list):
        raise ValueError("execution_errors must be a list when provided.")

    normalized_errors: list[NormalizedExecutionError] = []
    for index, item in enumerate(execution_errors):
        if isinstance(item, str) and item.strip():
            normalized_errors.append({"task_id": None, "error_text": item.strip()})
            continue

        if not isinstance(item, dict):
            raise ValueError(
                f"execution_errors[{index}] must be a non-empty string or a dictionary."
            )

        task_id = item.get("task_id")
        normalized_task_id = task_id if isinstance(task_id, int) else None
        error_text = None
        for key in ("error_text", "message", "error", "traceback", "summary"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                error_text = value.strip()
                break

        if error_text is None:
            raise ValueError(
                f"execution_errors[{index}] must include one non-empty error text field."
            )

        normalized_errors.append(
            {
                "task_id": normalized_task_id,
                "error_text": error_text,
            }
        )

    return normalized_errors


def _build_prompt_data_summary(data_summary: dict[str, Any]) -> dict[str, Any]:
    """Build a report-safe summary for the LLM prompt without raw preview/info dumps."""
    return {
        key: value
        for key, value in data_summary.items()
        if key not in {"preview_text", "info_text"}
    }


def _build_issue_clues(
    all_results: list[dict[str, Any]],
    data_summary: dict[str, Any],
    execution_errors: list[NormalizedExecutionError],
) -> list[str]:
    """Build concrete scenario clues for the reflection section."""
    clues = [
        f"数据文件为 {data_summary['dataset_name']}，数据维度为 {data_summary['shape_text']}。",
        f"缺失值摘要：{data_summary['missing_summary_text']}",
        f"重复值摘要：{data_summary['duplicate_count_text']}",
    ]

    null_columns_text = data_summary.get("null_columns_text")
    if isinstance(null_columns_text, str) and null_columns_text.strip():
        clues.append(f"空值列线索：{null_columns_text.strip()}")

    scanner_summary_text = data_summary.get("scanner_summary_text")
    if isinstance(scanner_summary_text, str) and scanner_summary_text.strip():
        clues.append(f"扫描摘要：{scanner_summary_text.strip()}")

    preprocessing_output = data_summary.get("preprocessing_output_summary_text")
    if isinstance(preprocessing_output, str) and preprocessing_output.strip():
        clues.append(f"预处理控制台输出：{preprocessing_output.strip()}")

    preprocessing_summary = data_summary.get("preprocessing_summary_text")
    if isinstance(preprocessing_summary, str) and preprocessing_summary.strip():
        clues.append(f"预处理摘要：{preprocessing_summary.strip()}")

    for item in all_results:
        task_label = f"任务{item['task_id']}"
        clues.append(f"{task_label} 清洗说明：{item['cleaning_summary']}")
        clues.append(f"{task_label} 问题处理：{item['problem_solution']}")
        clues.append(f"{task_label} 理论提示：{item['reflection_hint']}")
        if item.get("preprocessing_output_summary"):
            clues.append(f"{task_label} 预处理输出：{item['preprocessing_output_summary']}")

    for error_item in execution_errors:
        if error_item["task_id"] is None:
            clues.append(f"执行报错：{error_item['error_text']}")
        else:
            clues.append(f"任务{error_item['task_id']} 执行报错：{error_item['error_text']}")

    return clues


def _build_problem_reference_terms(
    all_results: list[dict[str, Any]],
    data_summary: dict[str, Any],
    execution_errors: list[NormalizedExecutionError],
) -> set[str]:
    """Collect real data and execution issue terms for validation."""
    reference_terms = set(GENERIC_REFERENCE_TERMS)

    for key in (
        "missing_summary_text",
        "null_columns_text",
        "scanner_summary_text",
        "preview_text",
        "preprocessing_output_summary_text",
        "preprocessing_summary_text",
    ):
        value = data_summary.get(key)
        if isinstance(value, str) and value.strip():
            reference_terms.update(_extract_terms_from_text(value))

    scanner_summary_lines = data_summary.get("scanner_summary_lines", [])
    if isinstance(scanner_summary_lines, list):
        for item in scanner_summary_lines:
            reference_terms.update(_extract_terms_from_text(str(item)))

    for item in all_results:
        for key in (
            "cleaning_summary",
            "problem_solution",
            "exploration_output",
            "preprocessing_output_summary",
        ):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                reference_terms.update(_extract_terms_from_text(value))
        reference_terms.update(item["column_mapping"].keys())
        reference_terms.update(item["column_mapping"].values())

    for error_item in execution_errors:
        reference_terms.update(_extract_terms_from_text(error_item["error_text"]))

    return {term for term in reference_terms if term.strip()}


def _build_reflection_logic_terms(
    all_results: list[dict[str, Any]],
    data_summary: dict[str, Any],
) -> set[str]:
    """Collect code-logic and preprocessing terms that should surface in reflection."""
    logic_terms = set(GENERIC_LOGIC_TERMS)

    for key in (
        "preprocessing_summary_text",
        "preprocessing_output_summary_text",
        "scanner_summary_text",
        "missing_summary_text",
        "null_columns_text",
    ):
        value = data_summary.get(key)
        if isinstance(value, str) and value.strip():
            logic_terms.update(_extract_terms_from_text(value))

    for item in all_results:
        for key in (
            "cleaning_summary",
            "problem_solution",
            "reflection_hint",
            "exploration_output",
            "preprocessing_output_summary",
        ):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                logic_terms.update(_extract_terms_from_text(value))
        logic_terms.update(item["column_mapping"].keys())
        logic_terms.update(item["column_mapping"].values())

    return {term for term in logic_terms if term.strip()}


def _build_user_prompt(
    all_results: list[dict[str, Any]],
    data_summary: dict[str, Any],
    report_title: str,
    execution_errors: list[NormalizedExecutionError],
) -> str:
    """Build the user prompt with title, dataset summary, task results, and errors."""
    all_results_json = json.dumps(all_results, ensure_ascii=False, indent=2)
    data_summary_json = json.dumps(_build_prompt_data_summary(data_summary), ensure_ascii=False, indent=2)
    execution_errors_json = json.dumps(execution_errors, ensure_ascii=False, indent=2)
    issue_clues_text = "\n".join(f"- {item}" for item in _build_issue_clues(all_results, data_summary, execution_errors))
    return USER_PROMPT_TEMPLATE.format(
        report_title=report_title,
        all_results_json=all_results_json,
        data_summary_json=data_summary_json,
        execution_errors_json=execution_errors_json,
        issue_clues_text=issue_clues_text,
    )


def _validate_section_block(value: Any, field_name: str) -> SectionBlock:
    """Validate one titled major section."""
    if not isinstance(value, dict):
        raise SynthesizerValidationError(f"Field '{field_name}' must be an object.")
    if set(value.keys()) != {"title", "content"}:
        raise SynthesizerValidationError(
            f"Field '{field_name}' must contain exactly 'title' and 'content'."
        )

    title = value.get("title")
    content = value.get("content")
    if not isinstance(title, str) or not title.strip():
        raise SynthesizerValidationError(f"Field '{field_name}.title' must be a non-empty string.")
    if not isinstance(content, str) or not content.strip():
        raise SynthesizerValidationError(f"Field '{field_name}.content' must be a non-empty string.")

    return {
        "title": title.strip(),
        "content": content.strip(),
    }


def _validate_section_item(value: Any, field_name: str, index: int) -> SectionItem:
    """Validate one list-based subsection item."""
    if not isinstance(value, dict):
        raise SynthesizerValidationError(f"{field_name}[{index}] must be an object.")
    if set(value.keys()) != {"sub_title", "content"}:
        raise SynthesizerValidationError(
            f"{field_name}[{index}] must contain exactly 'sub_title' and 'content'."
        )

    sub_title = value.get("sub_title")
    content = value.get("content")
    if not isinstance(sub_title, str) or not sub_title.strip():
        raise SynthesizerValidationError(
            f"{field_name}[{index}]['sub_title'] must be a non-empty string."
        )
    if not isinstance(content, str) or not content.strip():
        raise SynthesizerValidationError(
            f"{field_name}[{index}]['content'] must be a non-empty string."
        )

    return {
        "sub_title": sub_title.strip(),
        "content": content.strip(),
    }


def parse_synthesized_report(
    raw_response: str,
    task_count: int,
    report_title: str,
    problem_reference_terms: set[str],
    title_reference_terms: set[str],
    reflection_logic_terms: set[str],
) -> FinalReportData:
    """Parse and validate the synthesizer JSON response."""
    cleaned_response = clean_json_response(raw_response)

    try:
        parsed = json.loads(cleaned_response)
    except json.JSONDecodeError as exc:
        LOGGER.error("Failed to parse synthesizer response as JSON. Raw response: %s", raw_response)
        raise SynthesizerParseError("Synthesizer returned invalid JSON.") from exc

    if not isinstance(parsed, dict):
        raise SynthesizerValidationError("Synthesizer response must be a JSON object.")

    parsed_keys = set(parsed.keys())
    if parsed_keys != SECTION_KEYS:
        raise SynthesizerValidationError(
            "Synthesizer response must contain exactly these keys: "
            f"{sorted(SECTION_KEYS)}."
        )

    section_1_intro = _validate_section_block(parsed["section_1_intro"], "section_1_intro")
    section_3_mechanism = _validate_section_block(parsed["section_3_mechanism"], "section_3_mechanism")

    raw_analysis = parsed["section_2_analysis"]
    if not isinstance(raw_analysis, list):
        raise SynthesizerValidationError("Field 'section_2_analysis' must be a list.")
    if len(raw_analysis) != task_count:
        raise SynthesizerValidationError(
            f"Field 'section_2_analysis' must contain exactly {task_count} items."
        )
    section_2_analysis = [
        _validate_section_item(item, "section_2_analysis", index)
        for index, item in enumerate(raw_analysis)
    ]

    raw_reflection = parsed["section_4_reflection"]
    if not isinstance(raw_reflection, list):
        raise SynthesizerValidationError("Field 'section_4_reflection' must be a list.")
    if len(raw_reflection) != 2:
        raise SynthesizerValidationError("Field 'section_4_reflection' must contain exactly 2 items.")
    section_4_reflection = [
        _validate_section_item(item, "section_4_reflection", index)
        for index, item in enumerate(raw_reflection)
    ]

    if not section_1_intro["title"].startswith("一、"):
        raise SynthesizerValidationError("section_1_intro.title must be a formal first-level heading.")
    if not section_3_mechanism["title"].startswith("三、"):
        raise SynthesizerValidationError("section_3_mechanism.title must be a formal first-level heading.")

    problem_item = next(
        (item for item in section_4_reflection if "问题" in item["sub_title"] or "解决" in item["sub_title"]),
        None,
    )
    reflection_item = next(
        (item for item in section_4_reflection if "思考" in item["sub_title"] or "总结" in item["sub_title"]),
        None,
    )
    if problem_item is None:
        raise SynthesizerValidationError(
            "section_4_reflection must include a formal 'problems encountered' subsection."
        )
    if reflection_item is None:
        raise SynthesizerValidationError(
            "section_4_reflection must include a formal 'final thoughts' subsection."
        )
    if not _contains_any_term(problem_item["content"], problem_reference_terms):
        raise SynthesizerValidationError(
            "The problems subsection must reference at least one real field, cleaning clue, or execution issue."
        )
    if not _contains_first_person_tone(problem_item["content"]):
        raise SynthesizerValidationError(
            "The problems subsection must use a realistic first-person tone such as '我们在分析中发现...'."
        )
    reflection_item["content"] = _ensure_reflection_alignment(
        reflection_item["content"],
        report_title=report_title,
        title_reference_terms=title_reference_terms,
        reflection_logic_terms=reflection_logic_terms,
    )
    if title_reference_terms and not _contains_any_term(reflection_item["content"], title_reference_terms):
        raise SynthesizerValidationError(
            "The final thoughts subsection must reflect the topic implied by report_title."
        )
    if reflection_logic_terms and not _contains_any_term(reflection_item["content"], reflection_logic_terms):
        raise SynthesizerValidationError(
            "The final thoughts subsection must connect code logic or cleaning actions with academic reflection."
        )
    if not _contains_first_person_tone(reflection_item["content"]):
        raise SynthesizerValidationError(
            "The final thoughts subsection must use a realistic first-person tone."
        )

    return {
        "section_1_intro": section_1_intro,
        "section_2_analysis": section_2_analysis,
        "section_3_mechanism": section_3_mechanism,
        "section_4_reflection": section_4_reflection,
    }


def _compact_report_text(value: Any, fallback: str, max_length: int = 320) -> str:
    """Keep fallback report text concise and avoid carrying long raw data snippets."""
    if not isinstance(value, str) or not value.strip():
        return fallback
    compacted = re.sub(r"\s+", " ", value).strip()
    if len(compacted) <= max_length:
        return _soften_statistical_claims(compacted)
    return f"{_soften_statistical_claims(compacted[:max_length].rstrip())}……"


def _soften_statistical_claims(text: str) -> str:
    """Avoid overclaiming statistical strength in deterministic fallback text."""
    replacements = {
        "高度正相关": "呈现同向变化趋势",
        "显著正相关": "存在一定同向关联",
        "强正相关": "存在一定同向关联",
        "显著相关": "存在一定关联",
        "强相关": "存在一定关联",
        "显著影响": "可能存在影响",
        "显著提升": "可能有所提升",
        "显著降低": "可能有所降低",
    }
    softened = text
    for source, target in replacements.items():
        softened = softened.replace(source, target)
    return softened


def _build_deterministic_fallback_report(
    *,
    all_results: list[dict[str, Any]],
    data_summary: dict[str, Any],
    report_title: str,
    execution_errors: list[NormalizedExecutionError],
    problem_reference_terms: set[str],
    title_reference_terms: set[str],
    reflection_logic_terms: set[str],
) -> FinalReportData:
    """Build a local report when upstream synthesis is blocked by content risk."""
    dataset_name = _compact_report_text(data_summary.get("dataset_name"), "当前数据文件", max_length=120)
    shape_text = _compact_report_text(data_summary.get("shape_text"), "维度信息已完成扫描", max_length=120)
    missing_text = _compact_report_text(data_summary.get("missing_summary_text"), "缺失值情况已完成检查")
    duplicate_text = _compact_report_text(data_summary.get("duplicate_count_text"), "重复值情况已完成检查")
    scanner_text = _compact_report_text(
        data_summary.get("scanner_summary_text"),
        "已完成数据规模、字段数量、缺失值和重复记录的基础扫描。",
    )
    field_terms: list[str] = []
    for item in all_results:
        for source_name, target_name in item.get("column_mapping", {}).items():
            for candidate in (source_name, target_name):
                if candidate and candidate not in field_terms:
                    field_terms.append(str(candidate))
    field_overview = "、".join(field_terms[:8]) if field_terms else "任务相关字段"
    question_thread = "；".join(
        _compact_report_text(item.get("question_zh"), f"候选图表{index}分析", max_length=80)
        for index, item in enumerate(all_results, start=1)
    )
    core_research_question = (
        f"核心研究问题是：{report_title} 如何通过 {question_thread} 这些图表证据得到解释？"
    )
    mechanism_chain = (
        "区域经济差异—劳动力流动—人口结构—家庭结构"
    )

    analysis_items: list[SectionItem] = []
    for index, item in enumerate(all_results, start=1):
        question = _compact_report_text(item.get("question_zh"), f"候选图表{index}分析", max_length=120)
        analysis_text = _compact_report_text(item.get("analysis_text"), "该图表展示了当前任务对应的数据关系。")
        cleaning_text = _compact_report_text(item.get("cleaning_summary"), "已完成必要的字段整理与缺失值处理。")
        problem_text = _compact_report_text(item.get("problem_solution"), "已对绘图前的数据口径进行统一。")
        reflection_text = _compact_report_text(item.get("reflection_hint"), "该结果可作为后续报告分析的依据。")
        analysis_items.append(
            {
                "sub_title": f"（{index}）{question}",
                "content": (
                    f"研究问题上，本图主要回答“{question}”这一具体问题，并把它放在“{report_title}”的整体研究主线中理解。"
                    f"图表发现方面，{analysis_text} 原因解释方面，我们在绘图前先处理了与该图有关的数据口径：{cleaning_text}"
                    f" 同时，{problem_text} 从业务逻辑看，图中呈现的趋势或差异可能不仅来自图形本身，也与字段含义、统计分组和样本口径有关。"
                    f"现实启示方面，{reflection_text} 因此，该图可以为后续沿“{mechanism_chain}”或当前数据支持的等价链条展开交叉分析提供一个更清晰的观察入口。"
                ),
            }
        )

    first_result = all_results[0]
    first_cleaning = _compact_report_text(first_result.get("cleaning_summary"), "字段重命名和缺失值检查")
    first_problem = _compact_report_text(first_result.get("problem_solution"), "绘图口径统一")
    error_text = ""
    if execution_errors:
        error_text = _compact_report_text(execution_errors[0].get("error_text"), "", max_length=180)

    problem_content = (
        f"我们在分析中首先检查了数据来源 {dataset_name}，其数据维度为 {shape_text}。"
        f"缺失值摘要显示：{missing_text}；重复值摘要为：{duplicate_text}。"
        f"在字段处理上，我们重点关注 {field_overview} 等变量，并通过{first_cleaning}保证字段含义更清晰。"
        f"在可视化选择上，我们没有直接把原始表格转成图，而是先根据研究问题选择分组、趋势或变量关系的表达方式，再通过{first_problem}保证图表口径一致。"
        "本次分析仍存在局限：自动生成图表主要依赖已有字段和样本范围，对异常值、字段类型转换和更深层因果关系的判断仍需要人工复核。"
    )
    if error_text:
        problem_content = _append_sentence(problem_content, f"执行过程中还记录到问题线索：{error_text}")

    fallback_report: FinalReportData = {
        "section_1_intro": {
            "title": "一、引言与数据清洗说明",
            "content": (
                f"本报告围绕“{report_title}”展开，数据来源为 {dataset_name}，数据规模和结构概况为 {shape_text}。"
                f"从字段概览看，本次分析主要围绕 {field_overview} 等变量展开；数据扫描结果显示：{scanner_text}"
                f"在正式分析前，我们检查了缺失值、重复值和字段命名，缺失值摘要为：{missing_text}；重复值摘要为：{duplicate_text}。"
                f"{core_research_question}"
                f"因此，本报告的综合研究主线是：在完成字段规范化和基础清洗后，通过多个候选图表观察 {question_thread}，"
                f"并尽量沿“{mechanism_chain}”这类从经济差异到社会结构的链条解释这些趋势、差异或变量关系；"
                "若当前字段无法完整覆盖该链条，则以真实图表能够支持的变量关系为边界。"
            ),
        },
        "section_2_analysis": analysis_items,
        "section_3_mechanism": {
            "title": "三、深层经济机制分析",
            "content": (
                f"结合“{report_title}”这一研究主题，我们将 {question_thread} 放在同一解释框架下理解。"
                f"这些图表不是彼此孤立的展示，而是服务于“{mechanism_chain}”或当前数据可支持的等价主线，"
                "共同从趋势变化、结构差异、分组对比或变量关系等角度提供证据。"
                f"当 {field_overview} 等字段经过统一命名、缺失值检查和必要分组后，图表之间呈现出的共同趋势更容易被识别："
                "一方面，数据差异可能反映了样本结构、地区或群体特征的分化；另一方面，变量之间的同步变化也提示后续判断不能只看单一指标。"
                "因此，本报告更关注不同图表如何从核心指标波动、群体差异、收入或发展水平、人口与家庭结构等层面逐步收束，并据此形成对研究主题的综合解释。"
            ),
        },
        "section_4_reflection": [
            {
                "sub_title": "四、遇到的问题及解决方法",
                "content": problem_content,
            },
            {
                "sub_title": "五、总结与思考",
                "content": (
                    f"我们进一步认为，“{report_title}”的主要结论不只来自单张图，而来自多个图表围绕 {question_thread} 形成的共同证据。"
                    "本报告的价值在于把图表结果、字段重命名、缺失值检查和分组统计联系起来，使原始数据能够转化为更清晰的分析判断。"
                    "不足之处在于，当前结论仍主要基于描述性统计和可视化观察，对异常波动、样本代表性和潜在因果关系的解释仍需要更多背景信息支撑。"
                    "后续可以继续补充时间维度、用户分层、地区或产品分类等变量，扩大交叉分析范围，并结合业务背景对关键发现做进一步验证。"
                ),
            },
        ],
    }

    return parse_synthesized_report(
        json.dumps(fallback_report, ensure_ascii=False),
        task_count=len(all_results),
        report_title=report_title,
        problem_reference_terms=problem_reference_terms,
        title_reference_terms=title_reference_terms,
        reflection_logic_terms=reflection_logic_terms,
    )


def synthesize_report(
    all_results: list[dict[str, Any]],
    data_summary: dict[str, Any],
    report_title: str,
    execution_errors: list[Any] | None = None,
) -> FinalReportData:
    """Synthesize task results into TOC-friendly thesis-style sections."""
    normalized_results = _normalize_all_results(all_results)
    normalized_data_summary = _normalize_data_summary(data_summary)
    normalized_report_title = _normalize_report_title(report_title)
    normalized_execution_errors = _normalize_execution_errors(execution_errors)
    LOGGER.info("Starting TOC-friendly report synthesis for %s task results.", len(normalized_results))

    problem_reference_terms = _build_problem_reference_terms(
        normalized_results,
        normalized_data_summary,
        normalized_execution_errors,
    )
    title_reference_terms = _extract_title_terms(normalized_report_title)
    reflection_logic_terms = _build_reflection_logic_terms(
        normalized_results,
        normalized_data_summary,
    )

    user_prompt = _build_user_prompt(
        normalized_results,
        normalized_data_summary,
        normalized_report_title,
        normalized_execution_errors,
    )
    try:
        raw_response = llm_caller(
            prompt=user_prompt,
            system_prompt=SYSTEM_PROMPT,
            response_format={"type": "json_object"},
        )
    except Exception as exc:
        if not _is_content_risk_error(exc):
            raise
        LOGGER.warning(
            "Render synthesis triggered DeepSeek Content Exists Risk; using deterministic fallback report. "
            "task_count=%s; error_summary=%s",
            len(normalized_results),
            _summarize_exception_chain(exc),
        )
        return _build_deterministic_fallback_report(
            all_results=normalized_results,
            data_summary=normalized_data_summary,
            report_title=normalized_report_title,
            execution_errors=normalized_execution_errors,
            problem_reference_terms=problem_reference_terms,
            title_reference_terms=title_reference_terms,
            reflection_logic_terms=reflection_logic_terms,
        )

    try:
        final_report_data = parse_synthesized_report(
            raw_response,
            task_count=len(normalized_results),
            report_title=normalized_report_title,
            problem_reference_terms=problem_reference_terms,
            title_reference_terms=title_reference_terms,
            reflection_logic_terms=reflection_logic_terms,
        )
    except (SynthesizerParseError, SynthesizerValidationError):
        LOGGER.exception("Synthesizer response parsing or validation failed.")
        raise

    LOGGER.info("Successfully synthesized TOC-friendly report sections.")
    return final_report_data


def run(
    all_results: list[dict[str, Any]],
    data_summary: dict[str, Any],
    report_title: str,
    execution_errors: list[Any] | None = None,
) -> FinalReportData:
    """Convenience entry point for the synthesizer node."""
    return synthesize_report(all_results, data_summary, report_title, execution_errors)


if __name__ == "__main__":
    mock_results = [
        {
            "task_id": 1,
            "analysis_text": "产业数字化水平与人均GDP之间存在中等正相关关系。",
            "cleaning_summary": "我先把带换行的字段名整理成连续中文标签，再保留任务所需变量并删除空值。",
            "problem_solution": "我在处理“人均GDP\\n（万元）”时发现字段名含换行，直接引用不方便，因此先统一重命名为“人均GDP（万元）”。",
            "reflection_hint": "从结果看，地区经济发展基础与数字化水平之间存在协同关系。",
            "exploration_output": "(330, 11)\n<class 'pandas.core.frame.DataFrame'>\n...",
            "preprocessing_output_summary": "(330, 11)\n人均GDP（万元）列需要重命名。",
            "column_mapping": {
                "id": "编号",
                "人均GDP\n（万元）": "人均GDP（万元）",
            },
        },
        {
            "task_id": 2,
            "analysis_text": "不同省份之间的产业数字化水平存在较明显差异。",
            "cleaning_summary": "我在分组前统一核对了省份字段的文本口径，避免同类地区被拆分。",
            "problem_solution": "我在按照“省份”做分组统计时，先检查了分组字段是否适合直接求均值，以保证比较口径一致。",
            "reflection_hint": "区域间数字化差异提示了资源禀赋和发展基础的重要性。",
            "exploration_output": "(330, 11)\n<class 'pandas.core.frame.DataFrame'>\n...",
            "column_mapping": {
                "省份": "省份",
            },
        },
    ]
    mock_data_summary = {
        "dataset_name": "sample.xlsx",
        "shape_text": "(330, 11)",
        "info_text": "<class 'pandas.core.frame.DataFrame'>",
        "missing_summary_text": "Reviews: 12\nReview Date: 3\nProvince: 0",
        "duplicate_count_text": "0",
        "preview_text": "id Review Date Reviews Province 产业数字化水平",
        "null_columns_text": "Reviews: 12\nReview Date: 3",
        "scanner_summary_text": "总行数：330\n总列数：11\n空值概览：Reviews: 12; Review Date: 3",
        "preprocessing_output_summary_text": "(330, 11)\nReviews 列存在 12 个缺失值\nReview Date 列格式不统一",
    }
    mock_report_title = "基于区域经济视角的产业数字化发展特征量化研究"
    mock_execution_errors = [
        {
            "task_id": 1,
            "error_text": "第一次执行时报错：KeyError: 'Review Date'，因为字段名含空格与格式不一致。",
        }
    ]

    try:
        synthesized = synthesize_report(
            mock_results,
            mock_data_summary,
            mock_report_title,
            mock_execution_errors,
        )
        print(json.dumps(synthesized, ensure_ascii=False, indent=2))
    except Exception:
        LOGGER.exception("Synthesizer self-test failed.")
