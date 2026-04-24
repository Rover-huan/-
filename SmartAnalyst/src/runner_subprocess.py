"""Child process entry point for executing generated analysis code."""

from __future__ import annotations

import contextlib
import io
import math
import pickle
import sys
import traceback
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from src.node3_executor import (  # noqa: E402
    _collect_execution_outputs,
    _install_safe_savefig,
    np,
    pd,
    plt,
)


def _write_payload(output_path: Path, payload: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as file_obj:
        pickle.dump(payload, file_obj, protocol=pickle.HIGHEST_PROTOCOL)


def main(input_path_raw: str, output_path_raw: str) -> int:
    input_path = Path(input_path_raw)
    output_path = Path(output_path_raw)
    restore_savefig = None
    try:
        with input_path.open("rb") as file_obj:
            payload = pickle.load(file_obj)

        code_str = str(payload["code_str"])
        datasets = payload["datasets"]
        task_plan = payload["task_plan"]
        output_image_path = str(payload["output_image_path"])

        exec_context: dict[str, Any] = {
            "datasets": datasets,
            "output_image_path": output_image_path,
            "plt": plt,
            "pd": pd,
            "np": np,
            "math": math,
        }
        plt.close("all")
        restore_savefig = _install_safe_savefig(exec_context)
        stdout_buffer = io.StringIO()
        with contextlib.redirect_stdout(stdout_buffer):
            exec(code_str, exec_context)

        outputs = _collect_execution_outputs(
            task_plan=task_plan,
            exec_context=exec_context,
            exploration_output=stdout_buffer.getvalue().strip(),
        )
        _write_payload(output_path, {"ok": True, "outputs": outputs})
        return 0
    except Exception:
        _write_payload(output_path, {"ok": False, "traceback": traceback.format_exc()})
        return 1
    finally:
        if restore_savefig is not None:
            restore_savefig()
        plt.close("all")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python -m src.runner_subprocess <input.pkl> <output.pkl>", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
