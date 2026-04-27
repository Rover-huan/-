"""Node 1 utilities for dataset scanning, metadata extraction, and resilient LLM calls."""

from __future__ import annotations

import json
import logging
import os
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, TypedDict

import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        """Fallback no-op when python-dotenv is unavailable."""
        return False


OPENAI_IMPORT_ERROR: ImportError | None = None

try:
    from openai import APIConnectionError, APIError, OpenAI, RateLimitError
except ImportError as exc:
    OPENAI_IMPORT_ERROR = exc
    OpenAI = None  # type: ignore[assignment]

    class APIError(Exception):
        """Fallback API error when openai is unavailable."""

    class APIConnectionError(APIError):
        """Fallback connection error when openai is unavailable."""

    class RateLimitError(APIError):
        """Fallback rate limit error when openai is unavailable."""

from src.tabular_loader import load_excel_dataset
from service.usage import enforce_llm_budget, record_llm_call


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger().setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)

MODULE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_DIR.parent
ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_MODEL = "deepseek-v4-flash"


class DatasetMeta(TypedDict):
    """Preview metadata extracted from a single dataset."""

    dataset_name: str
    dataset_path: str
    columns: list[str]
    sample_data: list[dict[str, Any]]
    file_type: Literal["csv", "excel"]
    total_columns: int


class DatasetMetaBundle(TypedDict):
    """Combined metadata extracted from multiple datasets."""

    datasets: list[DatasetMeta]
    dataset_count: int
    dataset_names: list[str]
    dataset_columns_map: dict[str, list[str]]
    combined_summary_text: str
    columns: list[str]
    sample_data: list[dict[str, Any]]
    file_type: str
    total_columns: int


class Scanner:
    """Safely extract metadata from CSV and Excel files."""

    SAMPLE_ROWS = 3
    CSV_ENCODINGS = ("utf-8", "gbk", "latin1")

    @classmethod
    def extract_metadata(cls, file_path: str) -> DatasetMeta:
        """Safely extract preview metadata from a single file."""
        empty_result: DatasetMeta = {
            "dataset_name": "",
            "dataset_path": "",
            "columns": [],
            "sample_data": [],
            "file_type": "csv",
            "total_columns": 0,
        }
        if not file_path or not file_path.strip():
            LOGGER.error("[Node 1 错误] 输入文件路径为空，无法提取元数据。")
            return empty_result

        path = Path(file_path).expanduser().resolve()
        file_type: Literal["csv", "excel"] = "excel" if path.suffix.lower() in {".xls", ".xlsx"} else "csv"
        empty_result["dataset_name"] = path.name
        empty_result["dataset_path"] = path.as_posix()
        empty_result["file_type"] = file_type

        if not path.exists():
            LOGGER.error("[Node 1 错误] 数据文件不存在：%s", path)
            return empty_result

        if path.suffix.lower() == ".csv":
            dataframe = cls._read_csv_preview(path)
            file_type = "csv"
        elif path.suffix.lower() in {".xlsx", ".xls"}:
            dataframe = cls._read_excel_preview(path)
            file_type = "excel"
        else:
            LOGGER.error(
                "[Node 1 错误] 不支持的文件类型：%s。当前仅支持 .csv / .xlsx / .xls。",
                path.suffix,
            )
            return empty_result

        if dataframe is None:
            LOGGER.warning("[Node 1 警告] 文件预览读取失败，返回空元数据：%s", path)
            empty_result["file_type"] = file_type
            return empty_result

        metadata: DatasetMeta = {
            "dataset_name": path.name,
            "dataset_path": path.as_posix(),
            "columns": [str(column) for column in dataframe.columns.tolist()],
            "sample_data": cls._dataframe_to_records(dataframe),
            "file_type": file_type,
            "total_columns": int(dataframe.shape[1]),
        }
        LOGGER.info("Successfully extracted metadata for %s", path)
        return metadata

    @classmethod
    def extract_metadata_bundle(cls, file_paths: list[str]) -> DatasetMetaBundle:
        """Extract and combine metadata for multiple datasets."""
        normalized_paths = [
            str(Path(path).expanduser().resolve())
            for path in file_paths
            if isinstance(path, str) and path.strip()
        ]
        datasets: list[DatasetMeta] = []
        dataset_columns_map: dict[str, list[str]] = {}
        combined_columns: list[str] = []
        combined_column_seen: set[str] = set()
        summary_fragments: list[str] = []

        for file_path in normalized_paths:
            metadata = cls.extract_metadata(file_path)
            if not metadata["dataset_name"]:
                continue

            datasets.append(metadata)
            dataset_index = len(datasets)
            dataset_columns_map[metadata["dataset_name"]] = list(metadata["columns"])
            for column in metadata["columns"]:
                if column not in combined_column_seen:
                    combined_column_seen.add(column)
                    combined_columns.append(column)

            column_text = "、".join(metadata["columns"]) if metadata["columns"] else "无可用列信息"
            summary_fragments.append(
                f"数据集{dataset_index} [{metadata['dataset_name']}] 包含列: {column_text}"
            )

        file_types = {item["file_type"] for item in datasets if item["file_type"]}
        if not file_types:
            file_type = "unknown"
        elif len(file_types) == 1:
            file_type = next(iter(file_types))
        else:
            file_type = "mixed"

        return {
            "datasets": datasets,
            "dataset_count": len(datasets),
            "dataset_names": [item["dataset_name"] for item in datasets],
            "dataset_columns_map": dataset_columns_map,
            "combined_summary_text": "；".join(summary_fragments) if summary_fragments else "未发现可用数据集元数据。",
            "columns": combined_columns,
            "sample_data": datasets[0]["sample_data"] if datasets else [],
            "file_type": file_type,
            "total_columns": sum(int(item["total_columns"]) for item in datasets),
        }

    @classmethod
    def _read_csv_preview(cls, file_path: Path) -> pd.DataFrame | None:
        """Read a CSV preview defensively and return None on failure."""
        for index, encoding in enumerate(cls.CSV_ENCODINGS):
            try:
                dataframe = pd.read_csv(file_path, nrows=cls.SAMPLE_ROWS, encoding=encoding)
                if index > 0:
                    LOGGER.info(
                        "CSV preview succeeded using fallback encoding '%s' for %s",
                        encoding,
                        file_path,
                    )
                else:
                    LOGGER.info("CSV preview succeeded with utf-8 for %s", file_path)
                return dataframe
            except ImportError as exc:
                LOGGER.error(
                    "[Node 1 错误] 缺少读取 CSV 文件的必要依赖。请在终端运行: pip install pandas。文件：%s。详细信息：%s",
                    file_path,
                    exc,
                )
                return None
            except UnicodeDecodeError:
                LOGGER.warning(
                    "UnicodeDecodeError with encoding '%s' for %s; trying the next fallback.",
                    encoding,
                    file_path,
                )
            except Exception as exc:
                LOGGER.error(
                    "[Node 1 错误] CSV 文件预览读取失败，文件可能损坏、格式异常或编码不受支持。将跳过当前文件：%s。详细信息：%s",
                    file_path,
                    exc,
                )
                return None

        LOGGER.error(
            "[Node 1 错误] 无法使用 utf-8 / gbk / latin1 解码 CSV 文件，将跳过当前文件：%s",
            file_path,
        )
        return None

    @classmethod
    def _read_excel_preview(cls, file_path: Path) -> pd.DataFrame | None:
        """Read an Excel preview defensively and return None on failure."""
        try:
            dataframe = load_excel_dataset(file_path, preview_rows=cls.SAMPLE_ROWS)
            LOGGER.info("Excel preview succeeded for %s", file_path)
            return dataframe
        except ImportError as exc:
            if file_path.suffix.lower() == ".xls":
                LOGGER.error(
                    "[Node 1 错误] 缺少读取 .xls 文件的必要依赖。请在终端运行: pip install xlrd>=2.0.1。文件：%s。详细信息：%s",
                    file_path,
                    exc,
                )
            else:
                LOGGER.error(
                    "[Node 1 错误] 缺少读取 .xlsx 文件的必要依赖。请在终端运行: pip install openpyxl>=3.1.0。文件：%s。详细信息：%s",
                    file_path,
                    exc,
                )
            return None
        except Exception as exc:
            LOGGER.error(
                "[Node 1 错误] Excel 文件预览读取失败，文件可能损坏、格式异常或工作表不可读。将跳过当前文件：%s。详细信息：%s",
                file_path,
                exc,
            )
            return None

    @staticmethod
    def _dataframe_to_records(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert dataframe rows into JSON-friendly record dictionaries."""
        records: list[dict[str, Any]] = []
        for row in dataframe.to_dict(orient="records"):
            normalized_row = {
                str(key): Scanner._normalize_value(value)
                for key, value in row.items()
            }
            records.append(normalized_row)
        return records

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        """Normalize pandas and numpy values into JSON-friendly primitives."""
        if value is None:
            return None

        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass

        if isinstance(value, pd.Timestamp):
            return value.isoformat()

        item_method = getattr(value, "item", None)
        if callable(item_method):
            try:
                return item_method()
            except (ValueError, TypeError):
                return value

        return value


def run(file_path: str) -> DatasetMeta:
    """Convenience entry point for the scanner node."""
    return Scanner.extract_metadata(file_path)


def run_many(file_paths: list[str]) -> DatasetMetaBundle:
    """Convenience entry point for multi-dataset metadata extraction."""
    return Scanner.extract_metadata_bundle(file_paths)


@lru_cache(maxsize=1)
def _load_environment() -> bool:
    """Load .env variables once from the project root when python-dotenv is available."""
    if ENV_PATH.exists():
        loaded = load_dotenv(dotenv_path=ENV_PATH, override=False)
        LOGGER.info("Environment file processed from %s", ENV_PATH)
        return loaded

    LOGGER.warning(".env file not found at %s; using current process environment.", ENV_PATH)
    return False


def _clean_env_value(value: str | None) -> str | None:
    """Strip whitespace and matching wrapping quotes from environment values."""
    if value is None:
        return None

    cleaned = value.strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        cleaned = cleaned[1:-1].strip()

    return cleaned or None


def _strip_inline_comment(value: str) -> str:
    """Strip inline shell-style comments from unquoted .env values."""
    in_single_quote = False
    in_double_quote = False
    for index, character in enumerate(value):
        if character == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif character == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        elif character == "#" and not in_single_quote and not in_double_quote:
            if index == 0 or value[index - 1].isspace():
                return value[:index].rstrip()
    return value.rstrip()


@lru_cache(maxsize=1)
def _read_dotenv_values() -> dict[str, str]:
    """Parse SmartAnalyst/.env directly so config works even without python-dotenv."""
    if not ENV_PATH.exists():
        return {}

    parsed_values: dict[str, str] = {}
    raw_text = ENV_PATH.read_text(encoding="utf-8", errors="ignore")
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, raw_value = line.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            continue

        if normalized_key.lower().startswith("export "):
            normalized_key = normalized_key[7:].strip()
        if not normalized_key:
            continue

        stripped_value = _strip_inline_comment(raw_value.strip())
        cleaned_value = _clean_env_value(stripped_value)
        if cleaned_value is not None:
            parsed_values[normalized_key] = cleaned_value

    return parsed_values


def _get_config_value(name: str) -> str | None:
    """Read a config value with project .env taking precedence over inherited shell state."""
    _load_environment()
    dotenv_value = _read_dotenv_values().get(name)
    if dotenv_value:
        return dotenv_value
    return _clean_env_value(os.getenv(name))


def _get_model_name() -> str:
    """Return the configured model name or a safe default."""
    return _get_config_value("OPENAI_MODEL") or DEFAULT_MODEL


def _get_int_config_value(name: str, default: int) -> int:
    """Return one integer config value with fallback."""
    raw_value = _get_config_value(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        LOGGER.warning("Invalid integer value for %s=%r; falling back to %s.", name, raw_value, default)
        return default


def _is_llm_circuit_open() -> bool:
    """Return True when the global LLM circuit breaker is explicitly enabled."""
    raw_value = (_get_config_value("LLM_CIRCUIT_BREAKER_OPEN") or "").strip().lower()
    return raw_value in {"1", "true", "yes", "on"}


def llm_caller(
    prompt: str,
    system_prompt: str | None = None,
    response_format: dict[str, Any] | None = None,
) -> str:
    """Call the LLM with retries, exponential backoff, and detailed logging."""
    if not prompt or not prompt.strip():
        raise ValueError("prompt must be a non-empty string.")

    _load_environment()

    if OpenAI is None:
        raise RuntimeError(
            "The 'openai' package is not installed. Install dependencies from requirements.txt."
        ) from OPENAI_IMPORT_ERROR

    if _is_llm_circuit_open():
        raise RuntimeError(
            "LLM calls are temporarily disabled because LLM_CIRCUIT_BREAKER_OPEN is enabled."
        )

    api_key = _get_config_value("OPENAI_API_KEY")
    base_url = _get_config_value("OPENAI_BASE_URL")
    if not api_key:
        raise EnvironmentError(
            "A non-empty OPENAI_API_KEY is required. The value was not found in either the "
            "current process environment or SmartAnalyst/.env."
        )

    timeout_seconds = _get_int_config_value("OPENAI_TIMEOUT_SECONDS", 120)
    model_name = _get_model_name()
    attempt = 1
    max_attempts = _get_int_config_value("OPENAI_MAX_ATTEMPTS", 5)
    backoff_seconds = max(_get_int_config_value("OPENAI_BACKOFF_SECONDS", 1), 1)

    messages: list[dict[str, str]] = []
    if system_prompt and system_prompt.strip():
        messages.append({"role": "system", "content": system_prompt.strip()})
    messages.append({"role": "user", "content": prompt.strip()})

    call_started_at = time.perf_counter()
    while attempt <= max_attempts:
        attempt_started_at = time.perf_counter()
        LOGGER.info(
            "LLM call attempt %s/%s started for model '%s' with timeout=%ss.",
            attempt,
            max_attempts,
            model_name,
            timeout_seconds,
        )
        client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout_seconds, max_retries=0)
        try:
            enforce_llm_budget()
            request_payload: dict[str, Any] = {
                "model": model_name,
                "messages": messages,
            }
            if response_format is not None:
                request_payload["response_format"] = response_format

            response = client.chat.completions.create(**request_payload)
            message_content = response.choices[0].message.content
            if not message_content:
                raise RuntimeError("LLM returned an empty response.")

            record_llm_call()
            LOGGER.info(
                "LLM call succeeded on attempt %s/%s for model '%s' in %sms; total_elapsed_ms=%s.",
                attempt,
                max_attempts,
                model_name,
                int((time.perf_counter() - attempt_started_at) * 1000),
                int((time.perf_counter() - call_started_at) * 1000),
            )
            return message_content
        except (APIError, APIConnectionError, RateLimitError) as exc:
            attempt_duration_ms = int((time.perf_counter() - attempt_started_at) * 1000)
            will_retry = attempt < max_attempts
            error_type = type(exc).__name__
            is_timeout = "timeout" in error_type.lower() or "timed" in str(exc).lower()
            LOGGER.warning(
                "LLM call failed on attempt %s/%s for model '%s'; error_type=%s; "
                "duration_ms=%s; timeout_seconds=%s; will_retry=%s; backoff_seconds=%s; is_timeout=%s.",
                attempt,
                max_attempts,
                model_name,
                error_type,
                attempt_duration_ms,
                timeout_seconds,
                will_retry,
                min(backoff_seconds, 20) if will_retry else 0,
                is_timeout,
            )
            if attempt >= max_attempts:
                LOGGER.error(
                    "LLM call exhausted all retry attempts for model '%s' after %sms.",
                    model_name,
                    int((time.perf_counter() - call_started_at) * 1000),
                )
                raise

            time.sleep(min(backoff_seconds, 20))
            backoff_seconds = min(backoff_seconds * 2, 20)
            attempt += 1
        finally:
            close_method = getattr(client, "close", None)
            if callable(close_method):
                close_method()

    raise RuntimeError("LLM call failed after exhausting all retry attempts.")


def _ensure_self_test_csv(file_path: Path) -> None:
    """Create a tiny CSV fixture for the standalone scanner self-test."""
    if file_path.exists():
        return

    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(
        "customer,revenue,region\n"
        "Acme,1200,APAC\n"
        "Globex,980,EMEA\n"
        "Initech,1430,NA\n"
        "Umbrella,760,LATAM\n",
        encoding="utf-8",
        newline="\n",
    )
    LOGGER.info("Created self-test CSV fixture at %s", file_path)


if __name__ == "__main__":
    test_csv_path = (MODULE_DIR / ".." / "data" / "test.csv").resolve()
    _ensure_self_test_csv(test_csv_path)

    try:
        scanner_result = Scanner.extract_metadata(str(test_csv_path))
        LOGGER.info("Scanner single-file self-test result:\n%s", json.dumps(scanner_result, ensure_ascii=False, indent=2))
        bundle_result = Scanner.extract_metadata_bundle([str(test_csv_path)])
        LOGGER.info("Scanner multi-file self-test result:\n%s", json.dumps(bundle_result, ensure_ascii=False, indent=2))
    except Exception:
        LOGGER.exception("Scanner self-test failed.")

    try:
        llm_result = llm_caller(prompt="Hello")
        LOGGER.info("LLM self-test result: %s", llm_result)
    except Exception as exc:
        LOGGER.warning("LLM self-test could not complete: %s", exc)
