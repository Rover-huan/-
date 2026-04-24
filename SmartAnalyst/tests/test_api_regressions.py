from __future__ import annotations

from datetime import datetime

from service import api
from service.error_mapper import map_failure_details
from service.models import JobEvent


def test_api_delete_job_can_access_celery_app():
    assert hasattr(api, "celery_app")
    assert api.celery_app.main == "smartanalyst"


def test_error_mapper_hides_disallowed_import_raw_detail_by_default():
    details = map_failure_details(
        raw_message="Generated code contains disallowed import statements, which bypasses executor self-healing.",
        stage="analysis",
    )

    assert details is not None
    assert details.error_title == "分析服务处理异常"
    assert details.error_category == "executor_error"
    assert details.error_code == "analysis_code_import_blocked"
    assert "Generated code" not in details.user_message
    assert details.raw_detail is None


def test_error_mapper_returns_raw_detail_only_when_requested():
    raw_detail = "Generated code contains disallowed import statements\nTraceback: very long stack"
    details = map_failure_details(
        raw_message="Generated code contains disallowed import statements, which bypasses executor self-healing.",
        stage="analysis",
        include_raw_detail=True,
        raw_detail=raw_detail,
    )

    assert details is not None
    assert details.raw_detail == raw_detail


def test_error_mapper_maps_data_empty_error_to_executor_error():
    details = map_failure_details(
        raw_message=(
            "Task 2 failed after 3 attempts. DataEmptyError: "
            "数据清洗后数据量为0，无法绘图。请检查数据过滤条件。 "
            "Safe debug snapshot: {'frames': {'df': {'shape': [10, 3]}}}"
        ),
        stage="analysis",
    )

    assert details is not None
    assert details.error_category == "executor_error"
    assert details.error_code == "data_empty_after_filter"
    assert details.user_message == "分析代码生成的筛选条件过严，导致可绘图数据为空。"
    assert details.raw_detail is None


def test_error_mapper_data_empty_does_not_expose_raw_values():
    details = map_failure_details(
        raw_message=(
            "DataEmptyError: 数据清洗后数据量为0，无法绘图。"
            " Safe debug snapshot: secret-customer-a north-private"
        ),
        stage="analysis",
    )

    assert details is not None
    assert "secret-customer-a" not in details.user_message
    assert "north-private" not in details.user_message


def test_public_event_serializer_hides_raw_error_payload():
    event = JobEvent(
        id="event-1",
        job_id="job-1",
        level="error",
        event_type="job.analysis_failed",
        message="Generated code contains disallowed import statements, which bypasses executor self-healing.",
        payload_json={"traceback": "Traceback: secret stack"},
        created_at=datetime.utcnow(),
    )

    serialized = api._serialize_job_event(event)

    assert serialized.message == "系统在生成分析逻辑时遇到内部限制，本次任务未能完成。"
    assert serialized.payload_json is not None
    assert serialized.payload_json["error_code"] == "analysis_code_import_blocked"
    assert "traceback" not in serialized.payload_json
