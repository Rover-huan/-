"""SmartAnalyst pipeline entrypoints for CLI and background workers."""

from __future__ import annotations

import io
import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, Callable

import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency guard
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        """Fallback no-op when python-dotenv is unavailable."""
        return False

from src import (
    node1_scanner,
    node2_planner,
    node3_5_synthesizer,
    node3_6_polisher,
    node3_executor,
    node4_renderer,
)
from service.config import get_settings
from src.tabular_loader import load_excel_dataset


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger().setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)

ProgressCallback = Callable[[str, str, dict[str, Any] | None], None]

PROJECT_ROOT = Path(__file__).resolve().parent
ENV_PATH = PROJECT_ROOT / ".env"
DATA_DIR = PROJECT_ROOT / "data"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
REPORT_TITLE = "SmartAnalyst Economist Report"
SUPPORTED_DATA_EXTENSIONS = {".csv", ".xlsx", ".xls"}
CSV_ENCODINGS = ("utf-8", "gbk", "latin1")
MAX_DATASETS = 5
MIN_SUCCESSFUL_CANDIDATE_CHARTS = 3


def _announce_step(step_number: str, description: str) -> None:
    """Print a clear pipeline banner to the console and logs."""
    banner = f"--- [Step {step_number}]: {description} ---"
    print(banner)
    LOGGER.info(banner)


def _load_environment() -> None:
    """Load .env from the project root when available."""
    if ENV_PATH.exists():
        load_dotenv(dotenv_path=ENV_PATH, override=False)
        LOGGER.info("Environment loaded from %s", ENV_PATH)
    else:
        LOGGER.warning(".env file not found at %s; continuing with process environment.", ENV_PATH)


def _iter_supported_dataset_files(data_dir: Path) -> list[Path]:
    """Return supported files sorted by last modified time descending."""
    if not data_dir.exists():
        LOGGER.error("Data directory does not exist: %s", data_dir)
        raise FileNotFoundError(f"Data directory does not exist: {data_dir}")

    if not data_dir.is_dir():
        LOGGER.error("Configured data path is not a directory: %s", data_dir)
        raise NotADirectoryError(f"Configured data path is not a directory: {data_dir}")

    return sorted(
        (
            path
            for path in data_dir.iterdir()
            if path.is_file() and path.suffix.lower() in SUPPORTED_DATA_EXTENSIONS
        ),
        key=lambda path: (path.stat().st_mtime, path.name.lower()),
        reverse=True,
    )


def _load_full_dataframe(file_path: Path) -> pd.DataFrame:
    """Load one dataset with defensive CSV encoding fallback."""
    if not file_path.exists():
        raise FileNotFoundError(f"Dataset file does not exist: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        for index, encoding in enumerate(CSV_ENCODINGS):
            try:
                dataframe = pd.read_csv(file_path, encoding=encoding)
                if index > 0:
                    LOGGER.info(
                        "CSV full load for %s succeeded with fallback encoding '%s'.",
                        file_path.name,
                        encoding,
                    )
                return dataframe
            except UnicodeDecodeError:
                LOGGER.warning(
                    "Full CSV load failed with encoding '%s' for %s; trying the next fallback.",
                    encoding,
                    file_path.name,
                )
            except ImportError as exc:
                raise ImportError(
                    f"Missing pandas dependency required to read CSV files: {file_path}"
                ) from exc

        raise UnicodeDecodeError(
            "csv",
            b"",
            0,
            1,
            f"Unable to decode CSV file with encodings: {', '.join(CSV_ENCODINGS)}",
        )

    if suffix in {".xlsx", ".xls"}:
        try:
            return load_excel_dataset(file_path)
        except ImportError as exc:
            if suffix == ".xls":
                raise ImportError(
                    f"Reading .xls files requires xlrd>=2.0.1. File: {file_path}"
                ) from exc
            raise ImportError(
                f"Reading .xlsx files requires openpyxl>=3.1.0. File: {file_path}"
            ) from exc

    raise ValueError(f"Unsupported dataset type: {file_path.suffix}")


def _discover_dataset_paths(data_dir: Path, limit: int = MAX_DATASETS) -> list[Path]:
    """Discover up to N valid datasets from a directory."""
    candidate_files = _iter_supported_dataset_files(data_dir)
    if not candidate_files:
        supported_extensions = ", ".join(sorted(SUPPORTED_DATA_EXTENSIONS))
        raise FileNotFoundError(
            f"No supported data files found in {data_dir}. Expected one of: {supported_extensions}"
        )
    return [path.resolve() for path in candidate_files[:limit]]


def _load_datasets_from_paths(dataset_paths: list[Path]) -> dict[str, pd.DataFrame]:
    """Load all provided datasets into memory."""
    if not dataset_paths:
        raise ValueError("At least one dataset path is required.")

    datasets: dict[str, pd.DataFrame] = {}
    for dataset_path in dataset_paths:
        resolved_path = dataset_path.resolve()
        dataframe = _load_full_dataframe(resolved_path)
        datasets[resolved_path.name] = dataframe
        LOGGER.info("[Pipeline] Loaded dataset: %s", resolved_path.name)
    return datasets


def _discover_and_load_datasets(data_dir: Path, limit: int = MAX_DATASETS) -> tuple[list[Path], dict[str, pd.DataFrame]]:
    """Discover up to N valid datasets and load them into a dictionary."""
    discovered_paths = _discover_dataset_paths(data_dir, limit)
    datasets = _load_datasets_from_paths(discovered_paths)
    dataset_count_message = f"Loaded {len(discovered_paths)} dataset(s) for joint analysis."
    LOGGER.info(dataset_count_message)
    print(f"[Pipeline] {dataset_count_message}")
    return discovered_paths, datasets


def _build_single_load_code(file_path: Path, variable_index: int) -> list[str]:
    """Build notebook-friendly loading code for one dataset."""
    dataset_literal = repr(file_path.resolve().as_posix())
    dataset_key = repr(file_path.name)
    path_var = f"data_path_{variable_index}"

    if file_path.suffix.lower() == ".csv":
        return [
            f"{path_var} = Path({dataset_literal})",
            "for encoding in ('utf-8', 'gbk', 'latin1'):",
            "    try:",
            f"        datasets[{dataset_key}] = pd.read_csv({path_var}, encoding=encoding)",
            "        break",
            "    except UnicodeDecodeError:",
            "        continue",
            "else:",
            "    raise UnicodeDecodeError('csv', b'', 0, 1, 'Unable to decode CSV with utf-8/gbk/latin1.')",
        ]

    return [
        f"{path_var} = Path({dataset_literal})",
        f"datasets[{dataset_key}] = pd.read_excel({path_var})",
    ]


def _build_load_code(dataset_paths: list[Path]) -> str:
    """Build notebook-friendly multi-dataset loading code."""
    if not dataset_paths:
        return "datasets = {}\ndf = pd.DataFrame()"

    lines = [
        "from pathlib import Path",
        "datasets = {}",
    ]
    for index, file_path in enumerate(dataset_paths, start=1):
        lines.extend(_build_single_load_code(file_path, index))

    first_dataset_name = dataset_paths[0].name
    lines.extend(
        [
            f"df = datasets[{first_dataset_name!r}].copy()",
            "# df is only a quick alias for the first dataset; use datasets for formal multi-table analysis.",
        ]
    )
    return "\n".join(lines)


def _build_single_dataset_summary(file_path: Path, dataframe: pd.DataFrame) -> dict[str, Any]:
    """Build a reusable summary block for one dataset."""
    info_buffer = io.StringIO()
    dataframe.info(buf=info_buffer)
    row_count, column_count = (int(dimension) for dimension in dataframe.shape)
    missing_counts = dataframe.isna().sum()
    missing_summary_lines = [
        f"{column}: {int(count)}"
        for column, count in missing_counts.items()
        if int(count) > 0
    ]
    duplicate_count = int(dataframe.duplicated().sum())
    preview_text = dataframe.head(5).to_string(index=False)

    return {
        "dataset_path": file_path.resolve().as_posix(),
        "dataset_name": file_path.name,
        "file_type": file_path.suffix.lower().lstrip("."),
        "shape_text": str((row_count, column_count)),
        "info_text": info_buffer.getvalue().strip(),
        "row_count": row_count,
        "column_count": column_count,
        "missing_summary_text": "\n".join(missing_summary_lines) if missing_summary_lines else "No missing values.",
        "duplicate_count_text": str(duplicate_count),
        "preview_text": preview_text,
        "null_columns_text": "\n".join(missing_summary_lines) if missing_summary_lines else "No missing values.",
        "scanner_summary_lines": [
            f"Dataset file: {file_path.name}",
            f"Total rows: {row_count}",
            f"Total columns: {column_count}",
            f"Duplicate rows: {duplicate_count}",
            f"Columns with nulls: {len(missing_summary_lines)}",
            f"Null overview: {'; '.join(missing_summary_lines) if missing_summary_lines else 'No missing values.'}",
        ],
    }


def _build_combined_data_summary(dataset_paths: list[Path], datasets: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Build one renderer-friendly summary object for all participating datasets."""
    if not dataset_paths or not datasets:
        raise ValueError("At least one dataset is required to build the combined data summary.")

    dataset_summaries = [
        _build_single_dataset_summary(path, datasets[path.name])
        for path in dataset_paths
    ]

    total_rows = sum(int(item["row_count"]) for item in dataset_summaries)
    total_columns = sum(int(item["column_count"]) for item in dataset_summaries)
    total_duplicates = sum(int(item["duplicate_count_text"]) for item in dataset_summaries)
    distinct_file_types = {item["file_type"] for item in dataset_summaries}
    primary_dataset_path = dataset_paths[0].resolve()

    scanner_summary_lines = [f"Discovered {len(dataset_summaries)} dataset(s) for this joint analysis."]
    scanner_summary_lines.extend(
        f"{item['dataset_name']} | shape {item['shape_text']} | duplicate rows {item['duplicate_count_text']} | missing {item['missing_summary_text'].replace(chr(10), '; ')}"
        for item in dataset_summaries
    )

    return {
        "dataset_path": primary_dataset_path.as_posix(),
        "dataset_name": f"Combined analysis ({len(dataset_summaries)} dataset(s)): {', '.join(path.name for path in dataset_paths)}",
        "file_type": "mixed" if len(distinct_file_types) > 1 else next(iter(distinct_file_types)),
        "shape_text": "\n".join(
            f"{item['dataset_name']}: {item['shape_text']}" for item in dataset_summaries
        ),
        "info_text": "\n\n".join(
            f"[{item['dataset_name']}]\n{item['info_text']}" for item in dataset_summaries
        ),
        "missing_summary_text": "\n".join(
            f"[{item['dataset_name']}] {item['missing_summary_text']}" for item in dataset_summaries
        ),
        "duplicate_count_text": "\n".join(
            [f"Total duplicate rows: {total_duplicates}"]
            + [f"{item['dataset_name']}: {item['duplicate_count_text']}" for item in dataset_summaries]
        ),
        "preview_text": "\n\n".join(
            f"[{item['dataset_name']}]\n{item['preview_text']}" for item in dataset_summaries
        ),
        "load_code": _build_load_code(dataset_paths),
        "row_count_text": str(total_rows),
        "column_count_text": str(total_columns),
        "null_columns_text": "\n".join(
            f"[{item['dataset_name']}] {item['null_columns_text']}" for item in dataset_summaries
        ),
        "scanner_summary_text": "\n".join(scanner_summary_lines),
        "scanner_summary_lines": scanner_summary_lines,
        "dataset_summaries": dataset_summaries,
        "preprocessing_code_text": "",
        "preprocessing_output_summary_text": "",
        "preprocessing_summary_text": "",
    }


def _attach_scanner_summary_to_meta(
    dataset_meta: dict[str, Any],
    data_summary: dict[str, Any],
) -> dict[str, Any]:
    """Enrich combined metadata with shared data-summary strings for downstream planning."""
    enriched_meta = dict(dataset_meta)
    scanner_summary_lines = data_summary.get("scanner_summary_lines", [])
    enriched_meta["data_summary_strings"] = [
        str(item).strip()
        for item in scanner_summary_lines
        if str(item).strip()
    ]
    enriched_meta["row_count_text"] = str(data_summary.get("row_count_text", "")).strip()
    enriched_meta["column_count_text"] = str(data_summary.get("column_count_text", "")).strip()
    enriched_meta["null_columns_text"] = str(data_summary.get("null_columns_text", "")).strip()
    enriched_meta["duplicate_count_text"] = str(data_summary.get("duplicate_count_text", "")).strip()
    enriched_meta["dataset_name"] = str(data_summary.get("dataset_name", "")).strip()
    return enriched_meta


def _attach_preprocessing_payload(
    data_summary: dict[str, Any],
    execution_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Expose the first task's preprocessing code and outputs to downstream renderers."""
    if not execution_results:
        return dict(data_summary)

    first_result = min(execution_results, key=lambda item: int(item["task_id"]))
    enriched_summary = dict(data_summary)
    enriched_summary["preprocessing_code_text"] = str(
        first_result.get("preprocessing_code") or first_result.get("prepare_code", "")
    ).strip()
    enriched_summary["preprocessing_output_summary_text"] = str(
        first_result.get("preprocessing_output_summary") or first_result.get("exploration_output", "")
    ).strip()
    enriched_summary["preprocessing_summary_text"] = str(
        first_result.get("cleaning_summary", "")
    ).strip()
    return enriched_summary


def _build_render_results(
    task_plans: list[dict[str, Any]],
    execution_results: list[dict[str, Any]],
    report_text: dict[str, Any],
) -> list[dict[str, Any]]:
    """Merge planner and synthesizer context into renderer-ready loop items."""
    if len(task_plans) != len(execution_results):
        raise ValueError(
            "Renderer input mismatch: task_plans and execution_results must have the same length."
        )

    section_2_analysis = report_text.get("section_2_analysis")
    if not isinstance(section_2_analysis, list):
        raise ValueError("Renderer input mismatch: report_text['section_2_analysis'] must be a list.")
    if len(section_2_analysis) != len(execution_results):
        raise ValueError(
            "Renderer input mismatch: section_2_analysis length must match execution results."
        )

    ordered_task_plans = sorted(task_plans, key=lambda item: int(item["task_id"]))
    ordered_results = sorted(execution_results, key=lambda item: int(item["task_id"]))

    render_results: list[dict[str, Any]] = []
    for task_plan, execution_result, section_item in zip(
        ordered_task_plans,
        ordered_results,
        section_2_analysis,
    ):
        task_id = int(task_plan["task_id"])
        if task_id != int(execution_result["task_id"]):
            raise ValueError(
                "Renderer input mismatch: task plan task_id does not match execution result task_id."
            )

        synthesized_analysis = ""
        if isinstance(section_item, dict):
            raw_analysis = section_item.get("content", "")
            if isinstance(raw_analysis, str):
                synthesized_analysis = raw_analysis.strip()

        enriched_result = dict(execution_result)
        enriched_result["question_zh"] = str(task_plan["question_zh"]).strip()
        enriched_result["required_datasets"] = list(task_plan.get("required_datasets", []))
        if synthesized_analysis:
            enriched_result["analysis_text"] = synthesized_analysis
        render_results.append(enriched_result)

    return render_results


def get_dataset_meta(data_paths: list[Path]) -> dict[str, Any]:
    """Run Node 1 and return combined metadata for all datasets."""
    return node1_scanner.Scanner.extract_metadata_bundle([str(path) for path in data_paths])


def generate_plan(dataset_meta: dict[str, Any]) -> dict[str, Any]:
    """Run Node 2 and return the academic title plus task plans."""
    return node2_planner.plan_research(dataset_meta)


def synthesize_economist_report(
    results: list[dict[str, Any]],
    data_summary: dict[str, Any],
    report_title: str,
) -> dict[str, Any]:
    """Run Node 3.5 and return the synthesized narrative report."""
    return node3_5_synthesizer.synthesize_report(results, data_summary, report_title)


def _copy_datasets_for_chart_task(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Give each chart task independent DataFrame objects before threaded execution."""
    return {dataset_name: dataframe.copy(deep=True) for dataset_name, dataframe in datasets.items()}


def _resolve_chart_generation_concurrency() -> int:
    """Return the configured chart concurrency, with in-process execution kept serial."""
    settings = get_settings()
    if settings.runner_mode == "inprocess":
        return 1
    return max(1, min(int(settings.chart_generation_concurrency), 2))


def _execute_chart_task(
    *,
    datasets: dict[str, pd.DataFrame],
    task_plan: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    """Execute one candidate chart task with isolated mutable dataset copies."""
    return node3_executor.execute_task(
        datasets=_copy_datasets_for_chart_task(datasets),
        task_plan=task_plan,
        output_dir=output_dir,
    )


def select_task_subset(
    task_plans: list[dict[str, Any]],
    execution_results: list[dict[str, Any]],
    selected_task_ids: list[int] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Filter planner and executor outputs down to the selected tasks."""
    if not selected_task_ids:
        return list(task_plans), list(execution_results)

    selected_set = {int(item) for item in selected_task_ids}
    filtered_plans = [item for item in task_plans if int(item["task_id"]) in selected_set]
    filtered_results = [item for item in execution_results if int(item["task_id"]) in selected_set]
    if not filtered_plans or len(filtered_plans) != len(filtered_results):
        raise ValueError("Selected task subset is invalid or incomplete.")
    return filtered_plans, filtered_results


def run_analysis_phase(
    dataset_paths: list[Path],
    output_dir: str | Path,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Run scanner, planner, and executor for a specific job-scoped dataset list."""
    phase_started_at = time.perf_counter()

    def emit_progress(event_type: str, message: str, payload: dict[str, Any] | None = None) -> None:
        LOGGER.info("%s %s", event_type, payload or {})
        if progress_callback is not None:
            progress_callback(event_type, message, payload)

    def elapsed_ms(started_at: float) -> int:
        return int((time.perf_counter() - started_at) * 1000)

    _load_environment()
    resolved_output_dir = Path(output_dir).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    normalized_paths = [Path(path).resolve() for path in dataset_paths]

    started_at = time.perf_counter()
    datasets = _load_datasets_from_paths(normalized_paths)
    emit_progress(
        "analysis.data_loaded",
        "Analysis input datasets were loaded.",
        {"duration_ms": elapsed_ms(started_at), "dataset_count": len(datasets)},
    )

    started_at = time.perf_counter()
    dataset_meta = get_dataset_meta(normalized_paths)
    emit_progress(
        "analysis.metadata_scanned",
        "Dataset metadata scan completed.",
        {"duration_ms": elapsed_ms(started_at), "dataset_count": len(normalized_paths)},
    )

    started_at = time.perf_counter()
    data_summary = _build_combined_data_summary(normalized_paths, datasets)
    dataset_meta = _attach_scanner_summary_to_meta(dataset_meta, data_summary)
    emit_progress(
        "analysis.summary_built",
        "Dataset summary was built.",
        {"duration_ms": elapsed_ms(started_at), "dataset_count": len(normalized_paths)},
    )

    started_at = time.perf_counter()
    planner_output = generate_plan(dataset_meta)
    report_title = planner_output["report_title"]
    task_plans = planner_output["tasks"]
    emit_progress(
        "analysis.plan_generated",
        "Analysis plan and candidate chart tasks were generated.",
        {"duration_ms": elapsed_ms(started_at), "task_count": len(task_plans)},
    )

    total_tasks = len(task_plans)
    chart_concurrency = min(_resolve_chart_generation_concurrency(), max(total_tasks, 1))
    LOGGER.info(
        "Executing %s candidate chart task(s) with chart_generation_concurrency=%s.",
        total_tasks,
        chart_concurrency,
    )

    task_entries: list[dict[str, Any]] = []
    for index, task_plan in enumerate(task_plans, start=1):
        task_id = int(task_plan["task_id"])
        task_entries.append(
            {
                "index": index,
                "task_id": task_id,
                "task_plan": task_plan,
            }
        )

    results_by_index: dict[int, dict[str, Any]] = {}
    failed_by_index: dict[int, dict[str, Any]] = {}
    futures: dict[Future[dict[str, Any]], dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=chart_concurrency, thread_name_prefix="chart-gen") as executor:
        next_entry_index = 0

        def submit_next_chart() -> bool:
            nonlocal next_entry_index
            if next_entry_index >= total_tasks:
                return False
            entry = task_entries[next_entry_index]
            next_entry_index += 1
            task_plan = entry["task_plan"]
            task_id = int(entry["task_id"])
            index = int(entry["index"])
            entry["started_at"] = time.perf_counter()
            LOGGER.info(
                "Executing task %s/%s with task_id=%s using datasets=%s",
                index,
                total_tasks,
                task_id,
                ", ".join(task_plan.get("required_datasets", [])),
            )
            emit_progress(
                "analysis.chart_started",
                f"Candidate chart generation started for task {task_id}.",
                {"task_id": task_id, "index": index, "total": total_tasks},
            )
            future = executor.submit(
                _execute_chart_task,
                datasets=datasets,
                task_plan=task_plan,
                output_dir=resolved_output_dir,
            )
            futures[future] = entry
            return True

        for _ in range(chart_concurrency):
            submit_next_chart()

        try:
            while futures:
                done_futures, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in done_futures:
                    entry = futures.pop(future)
                    task_id = int(entry["task_id"])
                    index = int(entry["index"])
                    try:
                        execution_result = future.result()
                    except Exception as exc:
                        failure_payload = {
                            "task_id": task_id,
                            "index": index,
                            "total": total_tasks,
                            "duration_ms": elapsed_ms(float(entry["started_at"])),
                            "error_type": type(exc).__name__,
                            "error_message": str(exc),
                            "level": "warning",
                        }
                        failed_by_index[index] = failure_payload
                        LOGGER.warning(
                            "Candidate chart generation failed for task %s/%s with task_id=%s: %s",
                            index,
                            total_tasks,
                            task_id,
                            exc,
                            exc_info=True,
                        )
                        emit_progress(
                            "analysis.chart_failed",
                            f"Candidate chart generation failed for task {task_id}.",
                            failure_payload,
                        )
                        submit_next_chart()
                        continue

                    results_by_index[index] = execution_result
                    emit_progress(
                        "analysis.chart_completed",
                        f"Candidate chart generation completed for task {task_id}.",
                        {
                            "task_id": task_id,
                            "index": index,
                            "total": total_tasks,
                            "duration_ms": elapsed_ms(float(entry["started_at"])),
                        },
                    )
                    submit_next_chart()
        except Exception:
            executor.shutdown(wait=True, cancel_futures=True)
            raise

    minimum_required = min(MIN_SUCCESSFUL_CANDIDATE_CHARTS, total_tasks) if total_tasks else 0
    results = [
        results_by_index[index]
        for index in sorted(results_by_index)
    ]
    successful_task_plans = [
        task_entries[index - 1]["task_plan"]
        for index in sorted(results_by_index)
    ]
    failed_count = len(failed_by_index)
    success_count = len(results)
    if success_count < minimum_required:
        failed_task_ids = [
            int(failed_by_index[index]["task_id"])
            for index in sorted(failed_by_index)
        ]
        raise node3_executor.ExecutorError(
            "Candidate chart generation produced too few successful charts: "
            f"success_count={success_count}, minimum_required={minimum_required}, "
            f"failed_count={failed_count}, failed_task_ids={failed_task_ids}"
        )

    if failed_count:
        emit_progress(
            "analysis.chart_partial_success",
            "Analysis continued with successful candidate charts after some chart failures.",
            {
                "success_count": success_count,
                "failed_count": failed_count,
                "minimum_required": minimum_required,
                "total": total_tasks,
                "failed_task_ids": [
                    int(failed_by_index[index]["task_id"])
                    for index in sorted(failed_by_index)
                ],
                "level": "warning",
            },
        )

    data_summary = _attach_preprocessing_payload(data_summary, results)
    emit_progress(
        "analysis.phase_completed",
        "Analysis phase pipeline completed.",
        {
            "duration_ms": elapsed_ms(phase_started_at),
            "task_count": len(results),
            "success_count": success_count,
            "failed_count": failed_count,
            "minimum_required": minimum_required,
        },
    )

    return {
        "dataset_meta": dataset_meta,
        "dataset_paths": [path.as_posix() for path in normalized_paths],
        "task_plans": successful_task_plans,
        "report_title": report_title,
        "results": results,
        "data_summary": data_summary,
    }


def run_render_phase(
    task_plans: list[dict[str, Any]],
    execution_results: list[dict[str, Any]],
    data_summary: dict[str, Any],
    report_title: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Run synthesizer and renderer for the already executed task results."""
    _load_environment()
    resolved_output_dir = Path(output_dir).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    draft_report_text = synthesize_economist_report(execution_results, data_summary, report_title)
    docx_report_text = draft_report_text
    try:
        docx_report_text = node3_6_polisher.polish_report_text(draft_report_text)
    except Exception as exc:
        LOGGER.warning(
            "DeepSeek report polish failed; falling back to draft report_text. error_type=%s",
            type(exc).__name__,
            exc_info=True,
        )
        docx_report_text = draft_report_text

    draft_render_results = _build_render_results(task_plans, execution_results, draft_report_text)
    if docx_report_text is draft_report_text:
        docx_render_results = draft_render_results
    else:
        docx_render_results = _build_render_results(task_plans, execution_results, docx_report_text)

    report_artifacts = node4_renderer.render_report(
        docx_render_results,
        docx_report_text,
        data_summary,
        report_title or REPORT_TITLE,
        output_dir=resolved_output_dir,
    )
    notebook_path = node4_renderer.render_notebook(
        draft_render_results,
        draft_report_text,
        data_summary,
        report_title or REPORT_TITLE,
        output_dir=resolved_output_dir,
    )
    cleaning_summary_path = node4_renderer.render_data_summary(
        draft_render_results,
        data_summary,
        output_dir=resolved_output_dir,
    )

    return {
        "render_results": docx_render_results,
        "report_text": docx_report_text,
        "artifacts": {
            "docx_path": report_artifacts["docx_path"],
            "pdf_path": report_artifacts["pdf_path"],
            "notebook_path": notebook_path,
            "cleaning_summary_path": cleaning_summary_path,
        },
    }


def run_pipeline_from_paths(
    dataset_paths: list[Path],
    output_dir: str | Path,
    *,
    selected_task_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Execute the full SmartAnalyst workflow end to end for explicit file paths."""
    analysis_bundle = run_analysis_phase(dataset_paths, output_dir)
    selected_plans, selected_results = select_task_subset(
        analysis_bundle["task_plans"],
        analysis_bundle["results"],
        selected_task_ids,
    )
    render_bundle = run_render_phase(
        selected_plans,
        selected_results,
        analysis_bundle["data_summary"],
        analysis_bundle["report_title"],
        output_dir,
    )
    return {
        **analysis_bundle,
        **render_bundle,
    }


def run_pipeline(
    data_dir: str | Path = DATA_DIR,
    output_dir: str | Path = OUTPUTS_DIR,
    *,
    selected_task_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Execute the full SmartAnalyst workflow end to end from a data directory."""
    _announce_step("0", "Initializing environment")
    _load_environment()
    resolved_output_dir = Path(output_dir).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    _announce_step("1", "Scanner - discovering and loading up to 5 datasets")
    dataset_paths = _discover_dataset_paths(Path(data_dir).resolve(), MAX_DATASETS)

    _announce_step("2-4", "Scanner/Planner/Executor - preparing candidate tasks and charts")
    analysis_bundle = run_analysis_phase(dataset_paths, resolved_output_dir)

    selected_plans, selected_results = select_task_subset(
        analysis_bundle["task_plans"],
        analysis_bundle["results"],
        selected_task_ids,
    )

    _announce_step("5-6", "Synthesizer/Renderer - generating report artifacts")
    render_bundle = run_render_phase(
        selected_plans,
        selected_results,
        analysis_bundle["data_summary"],
        analysis_bundle["report_title"],
        resolved_output_dir,
    )

    return {
        **analysis_bundle,
        **render_bundle,
    }


def main() -> int:
    """CLI entry point for the SmartAnalyst master pipeline."""
    try:
        run_pipeline()
        print("--- [Pipeline Complete]: SmartAnalyst finished successfully ---")
        return 0
    except (FileNotFoundError, NotADirectoryError, ValueError) as exc:
        LOGGER.error("SmartAnalyst pipeline stopped: %s", exc)
        print("--- [Pipeline Failed]: Check logs above for details ---")
        return 1
    except Exception:
        LOGGER.exception("SmartAnalyst pipeline failed catastrophically.")
        print("--- [Pipeline Failed]: Check logs above for details ---")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
