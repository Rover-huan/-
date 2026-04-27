from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest

from src import node3_5_synthesizer, node3_6_polisher
from src.node3_5_synthesizer import ReportSynthesisModelError, synthesize_report


def _sample_results() -> list[dict]:
    return [
        {
            "task_id": 1,
            "question_zh": "销售额变化趋势分析",
            "analysis_text": "销售额整体呈现上升趋势，后期增长速度有所放缓。",
            "cleaning_summary": "我们先统一了销售额字段名称，并对缺失值进行了检查。",
            "problem_solution": "我们在处理销售额字段时使用数值转换，保证折线图口径一致。",
            "reflection_hint": "该趋势说明销售增长与阶段性需求变化高度正相关。",
            "exploration_output": "(12, 4)",
            "column_mapping": {"sales": "销售额", "month": "月份"},
        },
        {
            "task_id": 2,
            "question_zh": "不同地区销售额对比",
            "analysis_text": "不同地区之间存在显著相关，华东地区贡献较高。",
            "cleaning_summary": "我们检查了地区字段的空值，并统一了地区文本口径。",
            "problem_solution": "我们按地区分组汇总销售额，避免重复记录影响对比。",
            "reflection_hint": "地区差异提示后续资源配置需要结合市场基础。",
            "exploration_output": "(12, 4)",
            "column_mapping": {"region": "地区", "sales": "销售额"},
        },
    ]


def _sample_data_summary() -> dict:
    return {
        "dataset_name": "sales.xlsx",
        "shape_text": "(12, 4)",
        "info_text": "SECRET_INFO_SHOULD_NOT_APPEAR",
        "missing_summary_text": "销售额: 0\n地区: 0",
        "duplicate_count_text": "0",
        "preview_text": "SECRET_ROW_CUSTOMER Alice 999",
        "null_columns_text": "销售额: 0",
        "scanner_summary_text": "总行数: 12\n总列数: 4",
        "preprocessing_summary_text": "完成字段重命名和缺失值检查。",
    }


def _valid_llm_json() -> str:
    return json.dumps(
        {
            "section_1_intro": {
                "title": "一、引言与数据清洗说明",
                "content": "我们围绕销售趋势研究完成了数据清洗和字段检查。",
            },
            "section_2_analysis": [
                {
                    "sub_title": "（一）销售额变化趋势分析",
                    "content": "我们观察到销售额整体呈现上升趋势。",
                },
                {
                    "sub_title": "（二）不同地区销售额对比",
                    "content": "我们进一步比较发现地区之间存在差异。",
                },
            ],
            "section_3_mechanism": {
                "title": "三、深层经济机制分析",
                "content": "销售趋势和地区差异共同反映了需求结构变化。",
            },
            "section_4_reflection": [
                {
                    "sub_title": "四、遇到的问题及解决方法",
                    "content": "我们在分析中发现销售额字段需要进行字段重命名和缺失值检查。",
                },
                {
                    "sub_title": "五、总结与思考",
                    "content": (
                        "我们围绕基于消费行为视角的销售趋势量化研究，"
                        "结合字段重命名、缺失值检查和分组统计形成结论。"
                    ),
                },
            ],
        },
        ensure_ascii=False,
    )


def _llm_json_with_analysis_count(count: int) -> str:
    payload = json.loads(_valid_llm_json())
    analysis_items = [
        {
            "sub_title": f"候选图表{index}分析",
            "content": f"我们在第{index}张图中观察到图表发现、原因解释和现实启示。",
        }
        for index in range(1, count + 1)
    ]
    payload["section_2_analysis"] = analysis_items
    return json.dumps(payload, ensure_ascii=False)


def _patch_synthesis_fallback_setting(monkeypatch, enabled: bool) -> None:
    monkeypatch.setattr(
        node3_5_synthesizer,
        "get_settings",
        lambda: SimpleNamespace(report_synthesis_deterministic_fallback_enabled=enabled),
    )


def _fallback_kwargs() -> dict:
    results = _sample_results()
    data_summary = _sample_data_summary()
    execution_errors = []
    report_title = "基于消费行为视角的销售趋势量化研究"
    return {
        "all_results": results,
        "data_summary": data_summary,
        "report_title": report_title,
        "execution_errors": execution_errors,
        "problem_reference_terms": node3_5_synthesizer._build_problem_reference_terms(
            results,
            data_summary,
            execution_errors,
        ),
        "title_reference_terms": node3_5_synthesizer._extract_title_terms(report_title),
        "reflection_logic_terms": node3_5_synthesizer._build_reflection_logic_terms(results, data_summary),
        "analysis_title_sources": results,
    }


def test_deterministic_fallback_accepts_analysis_title_sources_argument():
    report = node3_5_synthesizer._build_deterministic_fallback_report(**_fallback_kwargs())

    assert set(report) == {
        "section_1_intro",
        "section_2_analysis",
        "section_3_mechanism",
        "section_4_reflection",
    }
    assert len(report["section_2_analysis"]) == len(_sample_results())
    assert len(report["section_4_reflection"]) == 2


def test_content_risk_raises_model_error_when_deterministic_fallback_disabled(monkeypatch, caplog):
    calls = []

    def fake_llm_caller(*, prompt, system_prompt, response_format):
        calls.append(prompt)
        raise RuntimeError(
            "Error code: 400 - {'error': {'message': 'Content Exists Risk', "
            "'type': 'invalid_request_error', 'param': None, 'code': 'invalid_request_error'}}"
        )

    monkeypatch.setattr("src.node3_5_synthesizer.llm_caller", fake_llm_caller)
    _patch_synthesis_fallback_setting(monkeypatch, enabled=False)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ReportSynthesisModelError) as exc_info:
            synthesize_report(
                _sample_results(),
                _sample_data_summary(),
                "基于消费行为视角的销售趋势量化研究",
            )

    assert len(calls) == 1
    error_text = str(exc_info.value)
    assert "model_service_error" in error_text
    assert "report synthesis" in error_text
    assert "Content Exists Risk" in error_text
    assert "unexpected keyword argument" not in error_text
    assert "Report synthesis model service error" in caplog.text
    assert "analysis_completed=true" in caplog.text
    assert "selected_charts_preserved=true" in caplog.text


def test_content_risk_uses_deterministic_fallback_when_enabled(monkeypatch, caplog):
    calls = []

    def fake_llm_caller(*, prompt, system_prompt, response_format):
        calls.append(prompt)
        raise RuntimeError(
            "Error code: 400 - {'error': {'message': 'Content Exists Risk', "
            "'type': 'invalid_request_error', 'param': None, 'code': 'invalid_request_error'}}"
        )

    monkeypatch.setattr("src.node3_5_synthesizer.llm_caller", fake_llm_caller)
    _patch_synthesis_fallback_setting(monkeypatch, enabled=True)

    with caplog.at_level(logging.WARNING):
        report = synthesize_report(
            _sample_results(),
            _sample_data_summary(),
            "基于消费行为视角的销售趋势量化研究",
        )

    assert len(calls) == 1
    assert set(report) == {
        "section_1_intro",
        "section_2_analysis",
        "section_3_mechanism",
        "section_4_reflection",
    }
    assert len(report["section_2_analysis"]) == 2
    assert len(report["section_4_reflection"]) == 2
    assert "数据来源" in report["section_1_intro"]["content"]
    assert "研究主线" in report["section_1_intro"]["content"]
    assert "核心研究问题" in report["section_1_intro"]["content"]
    assert "区域经济差异—劳动力流动—人口结构—家庭结构" in report["section_1_intro"]["content"]
    assert "区域经济差异—劳动力流动—人口结构—家庭结构" in report["section_3_mechanism"]["content"]
    report_text = json.dumps(report, ensure_ascii=False)
    assert "高度正相关" not in report_text
    assert "显著相关" not in report_text
    assert "强相关" not in report_text
    assert "显著影响" not in report_text
    for section_item in report["section_2_analysis"]:
        assert "图表发现" in section_item["content"]
        assert "原因解释" in section_item["content"]
        assert "现实启示" in section_item["content"]
    assert "共同" in report["section_3_mechanism"]["content"]
    assert "局限" in report["section_4_reflection"][0]["content"]
    assert "后续" in report["section_4_reflection"][1]["content"]
    assert "Content Exists Risk" in caplog.text
    assert "deterministic fallback" in caplog.text


def test_fallback_does_not_include_full_preview_or_info_text(monkeypatch):
    def fake_llm_caller(*, prompt, system_prompt, response_format):
        raise RuntimeError(
            "Error code: 400 - {'error': {'message': 'Content Exists Risk', "
            "'type': 'invalid_request_error'}}"
        )

    monkeypatch.setattr("src.node3_5_synthesizer.llm_caller", fake_llm_caller)
    _patch_synthesis_fallback_setting(monkeypatch, enabled=True)

    report = synthesize_report(
        _sample_results(),
        _sample_data_summary(),
        "基于消费行为视角的销售趋势量化研究",
    )
    report_text = json.dumps(report, ensure_ascii=False)

    assert "SECRET_ROW_CUSTOMER" not in report_text
    assert "Alice 999" not in report_text
    assert "SECRET_INFO_SHOULD_NOT_APPEAR" not in report_text
    assert "销售额变化趋势分析" in report_text
    assert "不同地区销售额对比" in report_text
    assert set(report) == {
        "section_1_intro",
        "section_2_analysis",
        "section_3_mechanism",
        "section_4_reflection",
    }
    assert len(report["section_2_analysis"]) == len(_sample_results())
    assert "核心研究问题" in report_text
    assert "区域经济差异—劳动力流动—人口结构—家庭结构" in report_text


@pytest.mark.parametrize("bad_count", [1, 3])
def test_section_2_count_mismatch_triggers_one_structure_repair_retry(monkeypatch, bad_count):
    calls = []
    responses = [_llm_json_with_analysis_count(bad_count), _valid_llm_json()]

    def fake_llm_caller(*, prompt, system_prompt, response_format):
        calls.append(prompt)
        return responses.pop(0)

    monkeypatch.setattr("src.node3_5_synthesizer.llm_caller", fake_llm_caller)
    _patch_synthesis_fallback_setting(monkeypatch, enabled=False)

    report = synthesize_report(
        _sample_results(),
        _sample_data_summary(),
        "基于消费行为视角的销售趋势量化研究",
    )

    assert len(calls) == 2
    repair_prompt = calls[1]
    assert "上一次输出的 section_2_analysis 数量错误" in repair_prompt
    assert "exactly 2" in repair_prompt
    assert "不要新增未选图" in repair_prompt
    assert "不要遗漏已选图" in repair_prompt
    assert "不要在每个图表小节重复 Notebook 说明" in repair_prompt
    assert "销售额变化趋势分析" in repair_prompt
    assert "不同地区销售额对比" in repair_prompt
    assert len(report["section_2_analysis"]) == 2


def test_structure_repair_valid_response_succeeds_and_cleans_generic_titles(monkeypatch):
    calls = []
    responses = [_llm_json_with_analysis_count(1), _llm_json_with_analysis_count(2)]

    def fake_llm_caller(*, prompt, system_prompt, response_format):
        calls.append(prompt)
        return responses.pop(0)

    monkeypatch.setattr("src.node3_5_synthesizer.llm_caller", fake_llm_caller)
    _patch_synthesis_fallback_setting(monkeypatch, enabled=False)

    report = synthesize_report(
        _sample_results(),
        _sample_data_summary(),
        "基于消费行为视角的销售趋势量化研究",
    )

    assert len(calls) == 2
    assert len(report["section_2_analysis"]) == 2
    report_text = json.dumps(report, ensure_ascii=False)
    assert "候选图表1分析" not in report_text
    assert "候选图表2分析" not in report_text
    assert "完整可复现代码已保留在同步生成的 Jupyter Notebook 文件中" not in report_text
    assert "销售额变化趋势分析" in report_text
    assert "不同地区销售额对比" in report_text


def test_structure_repair_failure_raises_model_response_error_without_default_fallback(monkeypatch, caplog):
    calls = []
    responses = [_llm_json_with_analysis_count(1), _llm_json_with_analysis_count(3)]

    def fake_llm_caller(*, prompt, system_prompt, response_format):
        calls.append(prompt)
        return responses.pop(0)

    monkeypatch.setattr("src.node3_5_synthesizer.llm_caller", fake_llm_caller)
    _patch_synthesis_fallback_setting(monkeypatch, enabled=False)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ReportSynthesisModelError) as exc_info:
            synthesize_report(
                _sample_results(),
                _sample_data_summary(),
                "基于消费行为视角的销售趋势量化研究",
            )

    assert len(calls) == 2
    error_text = str(exc_info.value)
    assert "model_response_error" in error_text
    assert "section_2_analysis item count mismatch" in error_text
    assert "expected_count=2" in error_text
    assert "deterministic fallback" not in caplog.text
    assert "internal_error" not in error_text


def test_structure_repair_failure_can_use_deterministic_fallback_when_enabled(monkeypatch, caplog):
    calls = []
    responses = [_llm_json_with_analysis_count(1), _llm_json_with_analysis_count(3)]

    def fake_llm_caller(*, prompt, system_prompt, response_format):
        calls.append(prompt)
        return responses.pop(0)

    monkeypatch.setattr("src.node3_5_synthesizer.llm_caller", fake_llm_caller)
    _patch_synthesis_fallback_setting(monkeypatch, enabled=True)

    with caplog.at_level(logging.WARNING):
        report = synthesize_report(
            _sample_results(),
            _sample_data_summary(),
            "基于消费行为视角的销售趋势量化研究",
        )

    assert len(calls) == 2
    assert len(report["section_2_analysis"]) == len(_sample_results())
    assert "deterministic fallback" in caplog.text


def test_normal_llm_response_still_uses_original_parse_path(monkeypatch):
    calls = []

    def fake_llm_caller(*, prompt, system_prompt, response_format):
        calls.append(prompt)
        return _valid_llm_json()

    monkeypatch.setattr("src.node3_5_synthesizer.llm_caller", fake_llm_caller)

    report = synthesize_report(
        _sample_results(),
        _sample_data_summary(),
        "基于消费行为视角的销售趋势量化研究",
    )

    assert len(calls) == 1
    assert "SECRET_ROW_CUSTOMER" not in calls[0]
    assert "SECRET_INFO_SHOULD_NOT_APPEAR" not in calls[0]
    assert report["section_1_intro"]["content"] == "我们围绕销售趋势研究完成了数据清洗和字段检查。"
    assert len(report["section_2_analysis"]) == 2
    assert report["section_2_analysis"][0]["content"] == "我们观察到销售额整体呈现上升趋势。"
    assert set(report) == {
        "section_1_intro",
        "section_2_analysis",
        "section_3_mechanism",
        "section_4_reflection",
    }


def test_report_synthesis_deterministic_fallback_config_defaults_false(monkeypatch):
    from service.config import get_settings

    get_settings.cache_clear()
    monkeypatch.delenv("REPORT_SYNTHESIS_DETERMINISTIC_FALLBACK_ENABLED", raising=False)

    assert get_settings().report_synthesis_deterministic_fallback_enabled is False

    get_settings.cache_clear()


def test_node3_6_polisher_still_falls_back_to_draft_report(monkeypatch):
    draft_report = {
        "section_1_intro": {"title": "一、引言与数据清洗说明", "content": "初稿正文。"},
        "section_2_analysis": [{"sub_title": "销售额变化趋势分析", "content": "初稿分析。"}],
        "section_3_mechanism": {"title": "三、深层经济机制分析", "content": "初稿机制。"},
        "section_4_reflection": [
            {"sub_title": "四、遇到的问题及解决方法", "content": "初稿问题。"},
            {"sub_title": "五、总结与思考", "content": "初稿总结。"},
        ],
    }
    monkeypatch.setattr(
        node3_6_polisher,
        "get_settings",
        lambda: SimpleNamespace(
            enable_deepseek_polish=True,
            deepseek_polish_api_key="test-key",
            deepseek_polish_max_input_chars=120000,
            deepseek_polish_base_url="https://api.deepseek.com/v1",
            deepseek_polish_model="deepseek-v4-pro",
            deepseek_polish_timeout_seconds=180,
        ),
    )

    def fail_polish(*args, **kwargs):
        raise RuntimeError("polish model temporarily unavailable")

    monkeypatch.setattr(node3_6_polisher, "_call_deepseek_polish", fail_polish)

    assert node3_6_polisher.polish_report_text(draft_report) is draft_report
