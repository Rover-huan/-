"""Map raw job failures to user-facing error details."""

from __future__ import annotations

from dataclasses import dataclass


DEFAULT_ACTIONS = [
    "检查文件是否能正常打开。",
    "重新上传常见格式文件，例如 Excel、CSV、PDF、Word。",
    "减少文件数量或拆分文件后重试。",
    "如果多次失败，请联系管理员并提供任务 ID。",
]


@dataclass(frozen=True)
class FailureDetails:
    error_title: str
    user_message: str
    error_category: str
    error_code: str
    suggested_actions: list[str]
    raw_detail: str | None = None


def _contains_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(needle in text for needle in needles)


def _actions(*items: str) -> list[str]:
    return list(items)


def map_failure_details(
    *,
    raw_message: str | None,
    stage: str | None = None,
    include_raw_detail: bool = False,
    raw_detail: str | None = None,
) -> FailureDetails | None:
    """Return localized, actionable failure details for one raw backend error."""
    if not raw_message and not raw_detail:
        return None

    raw_text = (raw_message or raw_detail or "").strip()
    normalized = raw_text.lower()

    if "generated code contains disallowed import statements" in normalized:
        details = FailureDetails(
            error_title="分析服务处理异常",
            user_message="系统在生成分析逻辑时遇到内部限制，本次任务未能完成。",
            error_category="executor_error",
            error_code="analysis_code_import_blocked",
            suggested_actions=_actions(
                "请稍后重试。",
                "如果多次失败，请联系管理员并提供任务 ID。",
            ),
        )
    elif _contains_any(
        normalized,
        (
            "dataemptyerror",
            "data_empty_after_filter",
            "数据清洗后数据量为0",
            "empty `data_plot`",
            "empty `df_clean`",
            "empty plotting vectors",
        ),
    ):
        details = FailureDetails(
            error_title="分析服务处理异常",
            user_message="分析代码生成的筛选条件过严，导致可绘图数据为空。",
            error_category="executor_error",
            error_code="data_empty_after_filter",
            suggested_actions=_actions(
                "请稍后重试，系统会重新生成更稳健的图表分析代码。",
                "如果多次失败，请联系管理员并提供任务 ID。",
            ),
        )
    elif _contains_any(normalized, ("timeout", "timed out", "deadline")):
        details = FailureDetails(
            error_title="分析请求超时",
            user_message="当前分析耗时较长，系统未能在限定时间内完成处理。",
            error_category="timeout",
            error_code="timeout",
            suggested_actions=_actions(
                "请稍后重试。",
                "减少文件数量或拆分文件后重试。",
                "如果多次失败，请联系管理员并提供任务 ID。",
            ),
        )
    elif _contains_any(normalized, ("rate limit", "llm", "model", "openai", "deepseek", "apierror")):
        details = FailureDetails(
            error_title="模型服务暂时不可用",
            user_message="系统调用分析模型时遇到临时问题，本次任务未能完成。",
            error_category="model_error",
            error_code="model_service_error",
            suggested_actions=_actions(
                "请稍后重试。",
                "如果多次失败，请联系管理员并提供任务 ID。",
            ),
        )
    elif _contains_any(
        normalized,
        (
            "unsupported file",
            "file type",
            "empty",
            "csv",
            "excel",
            "xlsx",
            "xls",
            "parse",
            "decode",
            "corrupt",
        ),
    ):
        details = FailureDetails(
            error_title="文件解析失败",
            user_message="系统未能读取或解析上传文件，本次任务未能完成。",
            error_category="file_error",
            error_code="file_parse_error",
            suggested_actions=DEFAULT_ACTIONS,
        )
    elif _contains_any(normalized, ("too large", "exceeds", "too many", "quota", "limit")):
        details = FailureDetails(
            error_title="文件或任务规模超出限制",
            user_message="上传内容较大或任务较复杂，系统暂时无法完成处理。",
            error_category="validation_error",
            error_code="job_size_or_limit_error",
            suggested_actions=_actions(
                "减少文件数量或拆分文件后重试。",
                "压缩数据表规模，移除无关工作表或空列。",
                "如果多次失败，请联系管理员并提供任务 ID。",
            ),
        )
    elif _contains_any(normalized, ("permission", "forbidden", "unauthorized", "credentials", "login")):
        details = FailureDetails(
            error_title="登录或权限异常",
            user_message="当前登录状态或账号权限无法完成本次操作。",
            error_category="permission_error",
            error_code="permission_error",
            suggested_actions=_actions(
                "请重新登录后再试。",
                "如果仍然失败，请联系管理员并提供任务 ID。",
            ),
        )
    elif _contains_any(
        normalized,
        ("generated code", "executor", "runner", "self-healing", "traceback", "python"),
    ):
        details = FailureDetails(
            error_title="分析服务处理异常",
            user_message="分析服务在处理数据时遇到内部异常，本次任务未能完成。",
            error_category="executor_error",
            error_code="executor_internal_error",
            suggested_actions=_actions(
                "请稍后重试。",
                "如果多次失败，请联系管理员并提供任务 ID。",
            ),
        )
    else:
        category = "executor_error" if stage == "analysis" else "unknown"
        details = FailureDetails(
            error_title="分析失败",
            user_message="本次任务未能完成。",
            error_category=category,
            error_code="internal_error",
            suggested_actions=DEFAULT_ACTIONS,
        )

    if include_raw_detail:
        return FailureDetails(
            error_title=details.error_title,
            user_message=details.user_message,
            error_category=details.error_category,
            error_code=details.error_code,
            suggested_actions=details.suggested_actions,
            raw_detail=(raw_detail or raw_text).strip() or None,
        )

    return details
