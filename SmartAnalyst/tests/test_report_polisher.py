from __future__ import annotations

import json
from types import SimpleNamespace

import main
from src import node3_6_polisher


def _settings(
    *,
    enabled: bool = True,
    api_key: str | None = "test-polish-key",
    max_input_chars: int = 120000,
) -> SimpleNamespace:
    return SimpleNamespace(
        enable_deepseek_polish=enabled,
        deepseek_polish_api_key=api_key,
        deepseek_polish_base_url="https://api.deepseek.com/v1",
        deepseek_polish_model="deepseek-v4-flash",
        deepseek_polish_timeout_seconds=180,
        deepseek_polish_max_input_chars=max_input_chars,
    )


def _sample_report() -> dict:
    return {
        "section_1_intro": {
            "title": "一、引言与数据清洗说明",
            "content": "我们先介绍数据和研究主题。",
        },
        "section_2_analysis": [
            {
                "sub_title": "（一）销售额变化趋势分析",
                "content": "销售额整体上升，后期增速放缓。",
            },
            {
                "sub_title": "（二）不同地区销售额对比",
                "content": "地区之间销售额存在差异。",
            },
        ],
        "section_3_mechanism": {
            "title": "三、深层经济机制分析",
            "content": "多个图表共同说明需求结构变化。",
        },
        "section_4_reflection": [
            {
                "sub_title": "四、遇到的问题及解决方法",
                "content": "我们检查了缺失值和字段类型。",
            },
            {
                "sub_title": "五、总结与思考",
                "content": "本次分析仍以描述性统计为主。",
            },
        ],
        "directory": {"content": "目录字段不应发送给 DeepSeek。"},
        "toc": "TOC_PLACEHOLDER_SHOULD_STAY",
        "catalogue": {"content": "目录占位符也不应被润色。"},
    }


def _expected_payload(report: dict) -> list[dict]:
    return [
        {"path": ["section_1_intro", "content"], "content": report["section_1_intro"]["content"]},
        {
            "path": ["section_2_analysis", 0, "content"],
            "content": report["section_2_analysis"][0]["content"],
        },
        {
            "path": ["section_2_analysis", 1, "content"],
            "content": report["section_2_analysis"][1]["content"],
        },
        {"path": ["section_3_mechanism", "content"], "content": report["section_3_mechanism"]["content"]},
        {
            "path": ["section_4_reflection", 0, "content"],
            "content": report["section_4_reflection"][0]["content"],
        },
        {
            "path": ["section_4_reflection", 1, "content"],
            "content": report["section_4_reflection"][1]["content"],
        },
    ]


def _polished_response(report: dict) -> str:
    payload = _expected_payload(report)
    return json.dumps(
        [
            {
                "path": item["path"],
                "content": f"润色后：{item['content']}",
            }
            for item in payload
        ],
        ensure_ascii=False,
    )


def test_disabled_returns_original_without_calling_deepseek(monkeypatch):
    report = _sample_report()
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings(enabled=False))

    def fail_if_called(*args, **kwargs):
        raise AssertionError("DeepSeek should not be called when polish is disabled.")

    monkeypatch.setattr(node3_6_polisher, "_call_deepseek_polish", fail_if_called)

    assert node3_6_polisher.polish_report_text(report) is report


def test_missing_api_key_returns_original(monkeypatch):
    report = _sample_report()
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings(api_key=None))

    def fail_if_called(*args, **kwargs):
        raise AssertionError("DeepSeek should not be called without an API key.")

    monkeypatch.setattr(node3_6_polisher, "_call_deepseek_polish", fail_if_called)

    assert node3_6_polisher.polish_report_text(report) is report


def test_empty_report_returns_original(monkeypatch):
    report = {}
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings())

    assert node3_6_polisher.polish_report_text(report) is report


def test_oversized_input_returns_original(monkeypatch):
    report = _sample_report()
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings(max_input_chars=3))

    def fail_if_called(*args, **kwargs):
        raise AssertionError("DeepSeek should not be called for oversized input.")

    monkeypatch.setattr(node3_6_polisher, "_call_deepseek_polish", fail_if_called)

    assert node3_6_polisher.polish_report_text(report) is report


def test_deepseek_failure_returns_original(monkeypatch):
    report = _sample_report()
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings())

    def raise_failure(*args, **kwargs):
        raise RuntimeError("temporary upstream failure")

    monkeypatch.setattr(node3_6_polisher, "_call_deepseek_polish", raise_failure)

    assert node3_6_polisher.polish_report_text(report) is report


def test_non_json_response_returns_original(monkeypatch):
    report = _sample_report()
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings())
    monkeypatch.setattr(node3_6_polisher, "_call_deepseek_polish", lambda *args, **kwargs: "not json")

    assert node3_6_polisher.polish_report_text(report) is report


def test_response_count_mismatch_returns_original(monkeypatch):
    report = _sample_report()
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings())
    one_item = json.dumps([_expected_payload(report)[0]], ensure_ascii=False)
    monkeypatch.setattr(node3_6_polisher, "_call_deepseek_polish", lambda *args, **kwargs: one_item)

    assert node3_6_polisher.polish_report_text(report) is report


def test_response_path_mismatch_returns_original(monkeypatch):
    report = _sample_report()
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings())
    payload = _expected_payload(report)
    payload[0] = {"path": ["section_1_intro", "title"], "content": "不应改标题"}
    monkeypatch.setattr(
        node3_6_polisher,
        "_call_deepseek_polish",
        lambda *args, **kwargs: json.dumps(payload, ensure_ascii=False),
    )

    assert node3_6_polisher.polish_report_text(report) is report


def test_valid_response_replaces_only_content_and_preserves_structure(monkeypatch):
    report = _sample_report()
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings())
    monkeypatch.setattr(
        node3_6_polisher,
        "_call_deepseek_polish",
        lambda *args, **kwargs: _polished_response(report),
    )

    polished = node3_6_polisher.polish_report_text(report)

    assert polished is not report
    assert polished["section_1_intro"]["title"] == report["section_1_intro"]["title"]
    assert polished["section_2_analysis"][0]["sub_title"] == report["section_2_analysis"][0]["sub_title"]
    assert polished["section_2_analysis"][1]["sub_title"] == report["section_2_analysis"][1]["sub_title"]
    assert len(polished["section_2_analysis"]) == len(report["section_2_analysis"])
    assert polished["directory"] == report["directory"]
    assert polished["toc"] == report["toc"]
    assert polished["catalogue"] == report["catalogue"]
    assert polished["section_1_intro"]["content"].startswith("润色后：")
    assert polished["section_2_analysis"][0]["content"].startswith("润色后：")
    assert polished["section_3_mechanism"]["content"].startswith("润色后：")
    assert polished["section_4_reflection"][1]["content"].startswith("润色后：")


def test_directory_fields_are_not_sent_to_deepseek(monkeypatch):
    report = _sample_report()
    captured_prompts: list[str] = []
    monkeypatch.setattr(node3_6_polisher, "get_settings", lambda: _settings())

    def capture_call(prompt, **kwargs):
        captured_prompts.append(prompt)
        return _polished_response(report)

    monkeypatch.setattr(node3_6_polisher, "_call_deepseek_polish", capture_call)

    node3_6_polisher.polish_report_text(report)

    assert captured_prompts
    assert "目录字段不应发送给 DeepSeek" not in captured_prompts[0]
    assert "TOC_PLACEHOLDER_SHOULD_STAY" not in captured_prompts[0]
    assert "目录占位符也不应被润色" not in captured_prompts[0]


def test_run_render_phase_polish_exception_keeps_docx_rendering_with_draft(monkeypatch, tmp_path):
    draft_report = _sample_report()
    captured: dict[str, object] = {}

    monkeypatch.setattr(main, "synthesize_economist_report", lambda *args, **kwargs: draft_report)

    def raise_unexpected(*args, **kwargs):
        raise RuntimeError("unexpected polish failure")

    monkeypatch.setattr(main.node3_6_polisher, "polish_report_text", raise_unexpected)
    monkeypatch.setattr(
        main,
        "_build_render_results",
        lambda task_plans, execution_results, report_text: [
            {
                "analysis_text": report_text["section_1_intro"]["content"],
                "source_code": "SOURCE_CODE_SHOULD_STAY",
            }
        ],
    )

    def fake_render_report(results, report_text, data_summary, report_title, output_dir):
        captured["docx_report_text"] = report_text
        captured["docx_results"] = results
        return {"docx_path": str(tmp_path / "Final_Report.docx"), "pdf_path": None}

    monkeypatch.setattr(main.node4_renderer, "render_report", fake_render_report)
    monkeypatch.setattr(main.node4_renderer, "render_notebook", lambda *args, **kwargs: str(tmp_path / "report.ipynb"))
    monkeypatch.setattr(
        main.node4_renderer,
        "render_data_summary",
        lambda *args, **kwargs: str(tmp_path / "cleaning.txt"),
    )

    render_bundle = main.run_render_phase([], [], {}, "测试报告", tmp_path)

    assert captured["docx_report_text"] is draft_report
    assert render_bundle["report_text"] is draft_report
    assert render_bundle["artifacts"]["docx_path"].endswith("Final_Report.docx")
    assert render_bundle["artifacts"]["notebook_path"].endswith("report.ipynb")
    assert render_bundle["artifacts"]["cleaning_summary_path"].endswith("cleaning.txt")


def test_run_render_phase_polish_success_only_affects_docx_artifact(monkeypatch, tmp_path):
    draft_report = _sample_report()
    polished_report = _sample_report()
    polished_report["section_1_intro"]["content"] = "润色后的 DOCX 正文。"
    captured: dict[str, object] = {}

    monkeypatch.setattr(main, "synthesize_economist_report", lambda *args, **kwargs: draft_report)
    monkeypatch.setattr(main.node3_6_polisher, "polish_report_text", lambda report_text: polished_report)

    def fake_build_render_results(task_plans, execution_results, report_text):
        return [
            {
                "analysis_text": report_text["section_1_intro"]["content"],
                "source_code": "SOURCE_CODE_SHOULD_STAY",
                "cleaning_summary": "CLEANING_SUMMARY_SHOULD_STAY",
            }
        ]

    monkeypatch.setattr(main, "_build_render_results", fake_build_render_results)

    def fake_render_report(results, report_text, data_summary, report_title, output_dir):
        captured["docx_report_text"] = report_text
        captured["docx_results"] = results
        return {"docx_path": str(tmp_path / "Final_Report.docx"), "pdf_path": None}

    def fake_render_notebook(results, report_text, data_summary, report_title, output_dir):
        captured["notebook_report_text"] = report_text
        captured["notebook_results"] = results
        return str(tmp_path / "report.ipynb")

    def fake_render_data_summary(results, data_summary, output_dir):
        captured["cleaning_results"] = results
        return str(tmp_path / "cleaning.txt")

    monkeypatch.setattr(main.node4_renderer, "render_report", fake_render_report)
    monkeypatch.setattr(main.node4_renderer, "render_notebook", fake_render_notebook)
    monkeypatch.setattr(main.node4_renderer, "render_data_summary", fake_render_data_summary)

    render_bundle = main.run_render_phase([], [], {}, "测试报告", tmp_path)

    assert captured["docx_report_text"] is polished_report
    assert captured["notebook_report_text"] is draft_report
    assert captured["docx_results"][0]["analysis_text"] == "润色后的 DOCX 正文。"
    assert captured["notebook_results"][0]["analysis_text"] == "我们先介绍数据和研究主题。"
    assert captured["cleaning_results"][0]["analysis_text"] == "我们先介绍数据和研究主题。"
    assert captured["docx_results"][0]["source_code"] == "SOURCE_CODE_SHOULD_STAY"
    assert captured["notebook_results"][0]["source_code"] == "SOURCE_CODE_SHOULD_STAY"
    assert captured["cleaning_results"][0]["cleaning_summary"] == "CLEANING_SUMMARY_SHOULD_STAY"
    assert render_bundle["report_text"] is polished_report
    assert render_bundle["render_results"][0]["analysis_text"] == "润色后的 DOCX 正文。"
