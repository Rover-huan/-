"""Helpers for normalizing spreadsheet-style tables into clean pandas DataFrames."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


HEADER_SCAN_ROWS = 12


def _clean_cell(value: Any) -> Any | None:
    """Normalize one raw spreadsheet cell."""
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None

    if isinstance(value, float) and value.is_integer():
        return int(value)

    return value


def _header_text(value: Any, fallback_index: int) -> str:
    """Convert one header cell into a stable column name."""
    cleaned = _clean_cell(value)
    if cleaned is None:
        return f"column_{fallback_index + 1}"

    if isinstance(cleaned, (int, float)) and not isinstance(cleaned, bool):
        if isinstance(cleaned, float) and cleaned.is_integer():
            return str(int(cleaned))
        return str(cleaned)

    text = str(cleaned).replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if re.search(r"[\u4e00-\u9fff]", text):
        text = text.replace(" ", "")
    return text or f"column_{fallback_index + 1}"


def _make_unique_headers(headers: list[str]) -> list[str]:
    """Ensure column names stay unique after normalization."""
    counts: dict[str, int] = {}
    unique_headers: list[str] = []
    for header in headers:
        base = header.strip() or "column"
        counts[base] = counts.get(base, 0) + 1
        if counts[base] == 1:
            unique_headers.append(base)
        else:
            unique_headers.append(f"{base}_{counts[base]}")
    return unique_headers


def detect_excel_header_row(raw_df: pd.DataFrame, scan_rows: int = HEADER_SCAN_ROWS) -> int:
    """Pick the most likely header row from the first N spreadsheet rows."""
    if raw_df.empty:
        return 0

    upper_bound = min(len(raw_df), max(scan_rows, 1))
    best_index = 0
    best_score = (-1, -1, -1)

    for row_index in range(upper_bound):
        row_values = [_clean_cell(item) for item in raw_df.iloc[row_index].tolist()]
        non_empty = [item for item in row_values if item is not None]
        if len(non_empty) < 2:
            continue

        text_like = sum(isinstance(item, str) for item in non_empty)
        numeric_like = sum(isinstance(item, (int, float)) and not isinstance(item, bool) for item in non_empty)
        score = (len(non_empty), text_like, numeric_like)
        if score > best_score:
            best_index = row_index
            best_score = score

    return best_index


def normalize_excel_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw spreadsheet rows into a clean rectangular table."""
    if raw_df.empty:
        return raw_df.copy()

    raw_df = raw_df.dropna(axis=1, how="all")
    if raw_df.empty:
        return raw_df.copy()

    header_row = detect_excel_header_row(raw_df)
    header_values = [
        _header_text(raw_df.iat[header_row, column_index], column_index)
        for column_index in range(raw_df.shape[1])
    ]
    header_values = _make_unique_headers(header_values)

    normalized = raw_df.iloc[header_row + 1 :].copy()
    normalized.columns = header_values
    normalized = normalized.dropna(axis=0, how="all").dropna(axis=1, how="all")
    normalized = normalized.reset_index(drop=True)
    return normalized


def load_excel_dataset(file_path: str | Path, preview_rows: int | None = None) -> pd.DataFrame:
    """Load one Excel file and normalize common yearbook-style header layouts."""
    path = Path(file_path)
    raw_df = pd.read_excel(path, header=None)
    normalized = normalize_excel_dataframe(raw_df)
    if preview_rows is not None:
        return normalized.head(preview_rows).copy()
    return normalized
