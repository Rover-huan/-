"""Scaffold the SmartAnalyst project workspace."""

from __future__ import annotations

import os
from pathlib import Path


REQUIREMENTS_CONTENT = """pandas==2.2.1
openpyxl==3.1.2
openai==1.14.2
python-dotenv==1.0.1
docxtpl==0.16.7
nbformat==5.10.3
"""

ENV_CONTENT = """OPENAI_API_KEY="replace-with-openai-api-key"
OPENAI_BASE_URL="https://api.deepseek.com/v1"
"""

MAIN_CONTENT = '''"""Entry point for the SmartAnalyst workflow."""\n\n\ndef main() -> None:\n    print("SmartAnalyst workspace is ready.")\n\n\nif __name__ == "__main__":\n    main()\n'''

PACKAGE_INIT_CONTENT = '"""SmartAnalyst source package."""\n'

NODE_TEMPLATE = '''"""SmartAnalyst workflow node: {node_name}."""\n\n\ndef run() -> None:\n    """Placeholder implementation for {node_name}."""\n    pass\n'''


def ensure_directory(path: Path) -> None:
    """Create a directory if it does not already exist."""
    path.mkdir(parents=True, exist_ok=True)
    print(f"[dir]  {path}")


def write_file_if_missing(path: Path, content: str) -> None:
    """Create a file with content only when it does not exist yet."""
    if path.exists():
        print(f"[skip] {path} already exists")
        return

    path.write_text(content, encoding="utf-8", newline="\n")
    print(f"[file] {path}")


def build_smartanalyst_workspace(base_dir: Path) -> Path:
    """Create the SmartAnalyst folder structure and seed files."""
    project_root = base_dir / "SmartAnalyst"
    src_dir = project_root / "src"

    directories = [
        project_root,
        project_root / "data",
        project_root / "outputs",
        project_root / "templates",
        src_dir,
    ]

    for directory in directories:
        ensure_directory(directory)

    files_to_create = {
        project_root / ".env": ENV_CONTENT,
        project_root / "requirements.txt": REQUIREMENTS_CONTENT,
        project_root / "main.py": MAIN_CONTENT,
        src_dir / "__init__.py": PACKAGE_INIT_CONTENT,
        src_dir / "node1_scanner.py": NODE_TEMPLATE.format(node_name="node1_scanner"),
        src_dir / "node2_planner.py": NODE_TEMPLATE.format(node_name="node2_planner"),
        src_dir / "node3_executor.py": NODE_TEMPLATE.format(node_name="node3_executor"),
        src_dir / "node4_renderer.py": NODE_TEMPLATE.format(node_name="node4_renderer"),
    }

    for file_path, content in files_to_create.items():
        write_file_if_missing(file_path, content)

    return project_root


def main() -> None:
    workspace_root = Path(os.getcwd())
    project_root = build_smartanalyst_workspace(workspace_root)
    print(f"\nSmartAnalyst scaffold complete: {project_root}")


if __name__ == "__main__":
    main()
