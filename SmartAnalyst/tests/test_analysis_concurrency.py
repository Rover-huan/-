from __future__ import annotations

import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

import main
from service.config import get_settings


def _task_plans(count: int) -> list[dict[str, Any]]:
    return [
        {
            "task_id": index,
            "question_zh": f"task {index}",
            "analysis_type": "trend",
            "required_datasets": ["sample.csv"],
            "x_axis_col": "x",
            "y_axis_col": "y",
            "x_axis_label_zh": "x",
            "y_axis_label_zh": "y",
        }
        for index in range(1, count + 1)
    ]


def _result(task_id: int) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "image_path": f"task_{task_id}.png",
        "analysis_text": "ok",
        "code_snippet": "plt.plot([1], [1])",
        "prepare_code": "df = datasets['sample.csv'].copy()",
        "plot_code": "plt.plot([1], [1])",
        "exploration_output": "ok",
        "preprocessing_code": "df = datasets['sample.csv'].copy()",
        "preprocessing_output_summary": "ok",
        "cleaning_summary": "ok",
        "problem_solution": "ok",
        "reflection_hint": "ok",
        "column_mapping": {},
    }


def _patch_analysis_inputs(monkeypatch: pytest.MonkeyPatch, task_count: int) -> list[tuple[str, str, dict | None]]:
    events: list[tuple[str, str, dict | None]] = []
    monkeypatch.setattr(main, "_load_environment", lambda: None)
    monkeypatch.setattr(
        main,
        "_load_datasets_from_paths",
        lambda paths: {"sample.csv": pd.DataFrame({"x": [1, 2], "y": [2, 4]})},
    )
    monkeypatch.setattr(main, "get_dataset_meta", lambda paths: {"datasets": []})
    monkeypatch.setattr(
        main,
        "_build_combined_data_summary",
        lambda paths, datasets: {
            "dataset_name": "sample",
            "preprocessing_code_text": "",
            "preprocessing_output_summary_text": "",
            "preprocessing_summary_text": "",
        },
    )
    monkeypatch.setattr(main, "_attach_scanner_summary_to_meta", lambda meta, summary: dict(meta))
    monkeypatch.setattr(
        main,
        "generate_plan",
        lambda meta: {"report_title": "test report", "tasks": _task_plans(task_count)},
    )
    return events


def _run_phase(tmp_path: Path, events: list[tuple[str, str, dict | None]]) -> dict[str, Any]:
    return main.run_analysis_phase(
        [tmp_path / "sample.csv"],
        tmp_path,
        progress_callback=lambda event_type, message, payload: events.append((event_type, message, payload)),
    )


def test_chart_generation_concurrency_default(monkeypatch: pytest.MonkeyPatch):
    get_settings.cache_clear()
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.delenv("CHART_GENERATION_CONCURRENCY", raising=False)
    monkeypatch.setenv("RUNNER_MODE", "subprocess")

    assert get_settings().chart_generation_concurrency == 2

    get_settings.cache_clear()


def test_chart_generation_concurrency_invalid_and_bounds(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.setenv("RUNNER_MODE", "subprocess")

    get_settings.cache_clear()
    monkeypatch.setenv("CHART_GENERATION_CONCURRENCY", "not-a-number")
    assert get_settings().chart_generation_concurrency == 2

    get_settings.cache_clear()
    monkeypatch.setenv("CHART_GENERATION_CONCURRENCY", "0")
    assert get_settings().chart_generation_concurrency == 1

    get_settings.cache_clear()
    monkeypatch.setenv("CHART_GENERATION_CONCURRENCY", "99")
    assert get_settings().chart_generation_concurrency == 2

    get_settings.cache_clear()


def test_chart_generation_concurrency_inprocess_forces_serial(monkeypatch: pytest.MonkeyPatch):
    get_settings.cache_clear()
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.setenv("RUNNER_MODE", "inprocess")
    monkeypatch.setenv("CHART_GENERATION_CONCURRENCY", "2")

    assert get_settings().chart_generation_concurrency == 1

    get_settings.cache_clear()


def test_run_analysis_phase_concurrency_one_behaves_serial(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    events = _patch_analysis_inputs(monkeypatch, task_count=3)
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(runner_mode="subprocess", chart_generation_concurrency=1),
    )
    lock = threading.Lock()
    active = 0
    max_active = 0
    call_order: list[int] = []

    def fake_execute_task(*, datasets, task_plan, output_dir):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
            call_order.append(int(task_plan["task_id"]))
        time.sleep(0.01)
        with lock:
            active -= 1
        return _result(int(task_plan["task_id"]))

    monkeypatch.setattr(main.node3_executor, "execute_task", fake_execute_task)

    result = _run_phase(tmp_path, events)

    assert max_active == 1
    assert call_order == [1, 2, 3]
    assert [item["task_id"] for item in result["results"]] == [1, 2, 3]


def test_run_analysis_phase_concurrency_two_runs_fake_tasks_in_parallel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    events = _patch_analysis_inputs(monkeypatch, task_count=4)
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(runner_mode="subprocess", chart_generation_concurrency=2),
    )
    lock = threading.Lock()
    active = 0
    max_active = 0

    def fake_execute_task(*, datasets, task_plan, output_dir):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return _result(int(task_plan["task_id"]))

    monkeypatch.setattr(main.node3_executor, "execute_task", fake_execute_task)

    result = _run_phase(tmp_path, events)

    assert max_active == 2
    assert [item["task_id"] for item in result["results"]] == [1, 2, 3, 4]


def test_run_analysis_phase_keeps_results_order_when_futures_finish_out_of_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    events = _patch_analysis_inputs(monkeypatch, task_count=3)
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(runner_mode="subprocess", chart_generation_concurrency=2),
    )

    def fake_execute_task(*, datasets, task_plan, output_dir):
        task_id = int(task_plan["task_id"])
        time.sleep({1: 0.06, 2: 0.01, 3: 0.01}[task_id])
        return _result(task_id)

    monkeypatch.setattr(main.node3_executor, "execute_task", fake_execute_task)

    result = _run_phase(tmp_path, events)
    completed_task_ids = [
        payload["task_id"]
        for event_type, _, payload in events
        if event_type == "analysis.chart_completed" and payload
    ]

    assert completed_task_ids[0] == 2
    assert [item["task_id"] for item in result["results"]] == [1, 2, 3]


def test_run_analysis_phase_emits_chart_events_with_durations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    events = _patch_analysis_inputs(monkeypatch, task_count=2)
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(runner_mode="subprocess", chart_generation_concurrency=2),
    )
    monkeypatch.setattr(
        main.node3_executor,
        "execute_task",
        lambda *, datasets, task_plan, output_dir: _result(int(task_plan["task_id"])),
    )

    _run_phase(tmp_path, events)

    started_payloads = [payload for event_type, _, payload in events if event_type == "analysis.chart_started"]
    completed_payloads = [payload for event_type, _, payload in events if event_type == "analysis.chart_completed"]

    assert len(started_payloads) == 2
    assert len(completed_payloads) == 2
    for payload in completed_payloads:
        assert payload is not None
        assert {"task_id", "index", "total", "duration_ms"}.issubset(payload)
        assert isinstance(payload["duration_ms"], int)


def test_run_analysis_phase_emits_chart_failed_and_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    events = _patch_analysis_inputs(monkeypatch, task_count=2)
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(runner_mode="subprocess", chart_generation_concurrency=1),
    )

    def fake_execute_task(*, datasets, task_plan, output_dir):
        task_id = int(task_plan["task_id"])
        if task_id == 2:
            raise RuntimeError("chart failed")
        return _result(task_id)

    monkeypatch.setattr(main.node3_executor, "execute_task", fake_execute_task)

    with pytest.raises(RuntimeError, match="chart failed"):
        _run_phase(tmp_path, events)

    failed_payloads = [payload for event_type, _, payload in events if event_type == "analysis.chart_failed"]
    assert len(failed_payloads) == 1
    assert failed_payloads[0]["task_id"] == 2
    assert failed_payloads[0]["error_type"] == "RuntimeError"


def test_run_analysis_phase_inprocess_runner_degrades_to_serial(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    events = _patch_analysis_inputs(monkeypatch, task_count=3)
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: SimpleNamespace(runner_mode="inprocess", chart_generation_concurrency=2),
    )
    lock = threading.Lock()
    active = 0
    max_active = 0

    def fake_execute_task(*, datasets, task_plan, output_dir):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.02)
        with lock:
            active -= 1
        return _result(int(task_plan["task_id"]))

    monkeypatch.setattr(main.node3_executor, "execute_task", fake_execute_task)

    _run_phase(tmp_path, events)

    assert max_active == 1
