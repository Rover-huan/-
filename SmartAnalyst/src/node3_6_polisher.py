"""Node 3.6 final report-body polishing with strict fallback behavior."""

from __future__ import annotations

import copy
import json
import logging
from typing import Any

try:
    from openai import OpenAI
except ImportError as exc:  # pragma: no cover - optional dependency guard
    OpenAI = None  # type: ignore[assignment]
    OPENAI_IMPORT_ERROR: ImportError | None = exc
else:
    OPENAI_IMPORT_ERROR = None

from service.config import get_settings


LOGGER = logging.getLogger(__name__)

SYSTEM_PROMPT = """
你是“SmartAnalyst 报告正文终稿优化助手”，专门负责将 SmartAnalyst 自动生成的量化分析报告正文初稿润色成可以直接提交的正文终稿。

你的任务不是重新分析数据，也不是修改封面，也不是生成目录。你只处理正文表达。
目标文风是“学生认真完成的量化分析课程结课报告终稿”：正式、自然、清楚，有分析、有解释、有反思，但不要写成期刊论文、政府报告或模型自动生成说明。

必须遵守：
1. 不处理封面。
2. 不生成目录。
3. 不修改目录。
4. 不添加目录占位符。
5. 不重新计算或推断任何统计结果。
6. 不修改变量名、样本量、系数、p 值、R²、显著性水平、模型名称、图表编号、表格编号。
7. 不改变原文研究主题、分析结论方向和图表含义。
8. 保留原有标题层级、图表引用、表格引用、Markdown、占位符、编号结构。
9. 只优化语言、段落衔接、学术表达、逻辑过渡和可读性。
10. 去除明显 AI 生硬感、模板感、机械感。
11. 不要输出修改说明，不要输出建议。
12. 如果某些内容明显是数据、代码、表格、图片占位符或图表引用，必须原样保留。
13. 不要虚构文献、数据来源、实证结果、政策背景或新的研究结论。
14. 不要新增不存在的图表、表格、小节或目录。
15. 不要删除原有有效正文内容。
16. 只允许改写输入 JSON 中每个对象的 content 字符串，不允许修改 path，不允许新增或删除对象。
17. 不要修改标题层级，不要修改图表编号、表格编号，不要新增图表或表格。
18. 绝对不要新增原文中不存在的统计量或计算结果，包括但不限于 Pearson 相关系数、p 值、回归系数、R²、年均增长率、倍数差异、排名、显著性判断、均值、峰值、区间、占比等。原文没有明确给出的数值，不得自行计算、估计、补充或暗示。
19. 如果原文已经给出统计量或数值，可以保留并润色表达，但不得改变数值、单位、方向、显著性含义或模型名称。
20. 不能擅自改变变量含义。可以删除或弱化原始字段编码，但不能把“一代户”改成“核心家庭”，不能把“二代户”改成“核心家庭”或“单人户”，不能把“家庭户总数”改成“单人户”，不能把“外来农业户籍人口失业率”改成“外地户籍失业率”，除非原文已经明确说明二者一致。

正文润色风格要求：
1. 降低模板感：删除、合并或自然改写重复出现的机械说明。尤其不要在每个小节反复出现“完整可复现代码已保留在同步生成的 Jupyter Notebook 文件中，DOCX 正文只保留方法说明、图表和文字分析”这类句子；如确需保留，只在合适位置自然出现一次。
2. 分析方法不要模板化：如果多个小节重复出现“完整可复现代码已保留在同步生成的 Jupyter Notebook 文件中，DOCX 正文只保留方法说明、图表和文字分析”，不要机械保留在每个小节。可以只自然保留一次，或改成更具体的方法说明，例如“本节采用折线图观察某指标的月度变化，并结合波动方向分析阶段性特征”。不得为了具体化而新增原文没有的统计量。
3. 去除字段编码感：如果正文中出现类似“5.3”“5.4_2”“100031”“78569”“502967”“203923”等明显来自原始列名或字段编码的括号说明，可以在不改变变量含义的前提下删除括号编码，或改写为原文已有的中文变量名。例如将“全国城镇调查失业率（5.3）与外来农业户籍人口失业率（5.4_2）”改为“全国城镇调查失业率与外来农业户籍人口失业率”。不要让报告标题、研究问题或段落开头看起来像程序拼接出来的字段名。
4. 去除字段拼接痕迹：如果分析方法中出现“重点观察 地区 这一指标”“围绕 2005 与 2022 的关系展开分析”“对 2022 进行比较”等明显由字段名拼接出的不自然表达，应改写为自然的方法说明，但必须保持原图表的分析对象不变。
5. 强化图表分析结构：每个图表分析段落尽量先描述图中结果，再解释可能原因，最后说明局限或后续分析方向。不要只给结论，也不要过度推断因果。
6. 严格降低因果表达强度：如果内容主要来自描述性统计、折线图、柱状图、散点图或简单分组对比，不要使用“证明”“印证”“驱动”“导致”“决定”“形成机制”“显著影响”“必然带来”等过强因果词。优先使用“可能说明”“呈现出”“反映出”“与……有关”“为后续分析提供线索”“从描述性结果看”“仍需进一步检验”等谨慎表达。
7. 增强课程报告口吻：语言应像本科课程结课报告，正式但不拔高；可以使用“本节”“从图中可以看出”“结合前文结果”“进一步来看”等自然衔接词。避免使用“业务逻辑”这类不适合当前报告语境的词，优先改为“分析逻辑”“经济含义”“现实含义”或“可能解释”。
8. 增强章节衔接：在相邻图表之间加入轻微过渡，让正文不是简单堆叠多张图。例如在总体失业率分析后自然引出特定群体，在群体差异后引出工资与劳动力分布，在工资和人口分析后引出家庭结构。只能基于原文已有主题和图表含义做衔接，不要新增不存在的分析对象。
9. 优化“遇到的问题及解决方法”：不要写成泛泛的数据问题清单，应尽量结合原文已有的真实处理过程，例如表头误读、缺失值、重复行、列名编码、地区名称不一致、类型转换等；语气像学生总结实际处理过程，而不是技术日志。
10. 如果 content 中已经包含“关键代码与处理过程说明”，只润色其表达和衔接；不得新增原文没有的代码步骤、库、函数、模型或算法，不得粘贴大段代码。若其中包含“完整可复现代码和每个 cell 的运行输出已保留在同步生成的 Jupyter Notebook 文件中”这类统一说明，应保留为一次自然表述，不要删除，也不要在其他小节重复扩散。
11. 优化“总结与思考”：总结应包含主要发现、报告价值、不足之处、后续改进方向。不足要具体，例如样本量、单一年份、缺少相关系数或回归检验、不能识别因果；后续方向可以提到未来增加年份、引入更多解释变量、开展相关或回归分析，但不得补充任何原文没有的相关系数、p 值、回归结果、显著性判断或其他新统计量。

你必须只返回合法 JSON，不要返回 Markdown，不要使用代码块，不要输出解释。
""".strip()

USER_PROMPT_TEMPLATE = """
下面是一份 SmartAnalyst 自动生成的量化分析报告正文 content 列表。请在不改变数据事实、图表含义、统计结果、标题结构、图表编号、表格编号的前提下，只对 content 文本进行最终润色。

你必须返回与输入完全相同数量、完全相同 path 的 JSON 数组。
每个元素格式如下：
{
  "path": [...],
  "content": "润色后的正文"
}

注意：
- 不要生成目录。
- 不要处理封面。
- 不要输出修改说明。
- 不要新增 path。
- 不要删除 path。
- 不要修改 path。
- 只修改 content 字符串。
- 返回内容必须是合法 JSON 数组。
- 不要使用 Markdown 代码块。
- 不要新增任何原文没有的统计量、数值、显著性判断、模型结果或排名。
- 不要擅自改变变量含义，只能删除字段编码或使用原文已有的中文变量名。

润色重点：
- 让正文更像学生认真完成的量化分析课程结课报告终稿，正式、自然、清楚，但不要像期刊论文或政府报告。
- 降低模板感，删除或合并反复出现的机械说明；不要在每个小节重复说明 notebook、代码留存或 DOCX 只保留文字分析。分析方法应尽量改成自然、具体的一句话，但不能新增原文没有的统计量。
- 去除字段编码感：将明显原始列名编码或字段编号括号说明自然化，但不要改变变量含义，不要改图表编号或表格编号。例如可以删除“5.3”“5.4_2”“100031”“203923”等括号编码，但不能把“一代户”“二代户”“家庭户总数”“外来农业户籍人口失业率”等变量改成另一种含义。
- 去除字段拼接痕迹：将“重点观察 地区 这一指标”“围绕 2005 与 2022 的关系展开分析”“对 2022 进行比较”等生硬表达改为自然方法说明，但必须保持原分析对象不变。
- 每个图表分析尽量形成“图中结果—可能原因—局限或后续方向”的自然段落结构。
- 禁止新增统计量：不得新增 Pearson 相关系数、p 值、回归系数、R²、年均增长率、倍数差异、排名、显著性判断、均值、峰值、区间、占比等原文没有明确给出的结果；原文已有数值可以保留，但不得改变。
- 控制因果表达：描述性统计或可视化结果只能谨慎表述，不要写成已经证明因果。避免“证明”“印证”“驱动”“导致”“决定”“形成机制”“显著影响”“必然带来”，优先使用“可能说明”“呈现出”“反映出”“与……有关”“为后续分析提供线索”“从描述性结果看”“仍需进一步检验”。
- 调整报告语气：避免“业务逻辑”这类不适合当前报告语境的词，优先改为“分析逻辑”“经济含义”“现实含义”或“可能解释”；不要写成营销文案。
- 在相邻图表之间加入轻微过渡，让正文像一篇连续报告，而不是图表说明堆叠。
- “遇到的问题及解决方法”要像学生总结实际数据处理过程，结合原文已有线索，不要写成泛泛清单或技术日志。
- “总结与思考”要覆盖主要发现、报告价值、具体不足和后续改进方向；可以提出未来可开展相关或回归分析，但不能说已经做了未出现在原文中的检验，也不能补充任何新的数值、显著性或模型结论。

输入 JSON：
<<<PAYLOAD_JSON>>>
""".strip()


def _log_context(task_id: str | None, job_id: str | None, content_chars: int | None = None) -> dict[str, Any]:
    context: dict[str, Any] = {
        "task_id": task_id,
        "job_id": job_id,
    }
    if content_chars is not None:
        context["content_chars"] = content_chars
    return context


def _extract_content_payload(report_text: dict[str, Any]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []

    section_1_intro = report_text.get("section_1_intro")
    if isinstance(section_1_intro, dict) and isinstance(section_1_intro.get("content"), str):
        payload.append({"path": ["section_1_intro", "content"], "content": section_1_intro["content"]})

    section_2_analysis = report_text.get("section_2_analysis")
    if isinstance(section_2_analysis, list):
        for index, item in enumerate(section_2_analysis):
            if isinstance(item, dict) and isinstance(item.get("content"), str):
                payload.append({"path": ["section_2_analysis", index, "content"], "content": item["content"]})

    section_3_mechanism = report_text.get("section_3_mechanism")
    if isinstance(section_3_mechanism, dict) and isinstance(section_3_mechanism.get("content"), str):
        payload.append({"path": ["section_3_mechanism", "content"], "content": section_3_mechanism["content"]})

    section_4_reflection = report_text.get("section_4_reflection")
    if isinstance(section_4_reflection, list):
        for index, item in enumerate(section_4_reflection):
            if isinstance(item, dict) and isinstance(item.get("content"), str):
                payload.append({"path": ["section_4_reflection", index, "content"], "content": item["content"]})

    return payload


def _content_length(payload: list[dict[str, Any]]) -> int:
    return sum(len(str(item.get("content", ""))) for item in payload)


def _build_user_prompt(payload: list[dict[str, Any]]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    return USER_PROMPT_TEMPLATE.replace("<<<PAYLOAD_JSON>>>", payload_json)


def _call_deepseek_polish(prompt: str, *, api_key: str, base_url: str, model: str, timeout_seconds: int) -> str:
    if OpenAI is None:
        raise RuntimeError("The 'openai' package is not installed.") from OPENAI_IMPORT_ERROR

    client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout_seconds, max_retries=0)
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        message_content = response.choices[0].message.content
        if not message_content or not message_content.strip():
            raise ValueError("DeepSeek polish returned an empty response.")
        return message_content
    finally:
        close_method = getattr(client, "close", None)
        if callable(close_method):
            close_method()


def _parse_polish_response(raw_response: str, expected_payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parsed = json.loads(raw_response)
    if not isinstance(parsed, list):
        raise ValueError("DeepSeek polish response must be a JSON array.")
    if len(parsed) != len(expected_payload):
        raise ValueError("DeepSeek polish response length does not match input length.")

    validated: list[dict[str, Any]] = []
    for index, (actual, expected) in enumerate(zip(parsed, expected_payload)):
        if not isinstance(actual, dict):
            raise ValueError(f"DeepSeek polish item {index} must be an object.")
        if set(actual.keys()) != {"path", "content"}:
            raise ValueError(f"DeepSeek polish item {index} must contain only path and content.")
        if actual["path"] != expected["path"]:
            raise ValueError(f"DeepSeek polish item {index} path does not match input path.")
        content = actual["content"]
        if not isinstance(content, str) or not content.strip():
            raise ValueError(f"DeepSeek polish item {index} content must be a non-empty string.")
        validated.append({"path": actual["path"], "content": content.strip()})
    return validated


def _set_content_at_path(report_text: dict[str, Any], path: list[Any], content: str) -> None:
    target: Any = report_text
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = content


def polish_report_text(
    report_text: dict[str, Any],
    *,
    task_id: str | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Polish report content fields only, falling back to the original report on any issue."""
    settings = get_settings()
    if not settings.enable_deepseek_polish:
        LOGGER.info("report.polish.skipped DeepSeek polish disabled. %s", _log_context(task_id, job_id))
        return report_text

    if not report_text:
        LOGGER.info("report.polish.skipped report_text is empty. %s", _log_context(task_id, job_id))
        return report_text

    if not settings.deepseek_polish_api_key:
        LOGGER.warning(
            "report.polish.skipped DeepSeek polish skipped due to missing API key. %s",
            _log_context(task_id, job_id),
        )
        return report_text

    payload = _extract_content_payload(report_text)
    if not payload:
        LOGGER.info("report.polish.skipped no content fields were found. %s", _log_context(task_id, job_id))
        return report_text

    content_chars = _content_length(payload)
    context = _log_context(task_id, job_id, content_chars)
    if content_chars > settings.deepseek_polish_max_input_chars:
        LOGGER.warning(
            "report.polish.skipped DeepSeek polish skipped due to oversized input. max_chars=%s; %s",
            settings.deepseek_polish_max_input_chars,
            context,
        )
        return report_text

    LOGGER.info("report.polish.started DeepSeek polish started. %s", context)
    try:
        raw_response = _call_deepseek_polish(
            _build_user_prompt(payload),
            api_key=settings.deepseek_polish_api_key,
            base_url=settings.deepseek_polish_base_url,
            model=settings.deepseek_polish_model,
            timeout_seconds=settings.deepseek_polish_timeout_seconds,
        )
        polished_payload = _parse_polish_response(raw_response, payload)
        polished_report = copy.deepcopy(report_text)
        for item in polished_payload:
            _set_content_at_path(polished_report, item["path"], item["content"])
    except Exception as exc:
        LOGGER.warning(
            "report.polish.failed_fallback DeepSeek polish failed; falling back to draft report text. "
            "error_type=%s; %s",
            type(exc).__name__,
            context,
        )
        return report_text

    LOGGER.info("report.polish.succeeded DeepSeek polish succeeded. %s", context)
    return polished_report
