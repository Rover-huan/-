from __future__ import annotations

import builtins
import logging
import zipfile
from dataclasses import dataclass

from docx import Document

from src import node4_renderer


@dataclass(frozen=True)
class _FakeSettings:
    enable_auto_toc: bool
    auto_toc_backend: str


def _read_docx_member(docx_path, member_name: str) -> str:
    with zipfile.ZipFile(docx_path) as archive:
        return archive.read(member_name).decode("utf-8")


def test_inserted_toc_field_is_present_in_docx(tmp_path):
    docx_path = tmp_path / "toc.docx"
    document = Document()

    node4_renderer._configure_document_styles(document)
    node4_renderer._enable_update_fields_on_open(document)
    node4_renderer._insert_toc_page(document)
    node4_renderer._add_section_heading(
        document,
        "一、引言与数据清洗说明",
        level=1,
        start_on_new_page=False,
    )
    node4_renderer._add_section_heading(
        document,
        "（一）销售额变化趋势分析",
        level=2,
        start_on_new_page=False,
    )
    document.save(docx_path)

    document_xml = _read_docx_member(docx_path, "word/document.xml")

    assert 'TOC \\o "1-2" \\h \\z \\u' in document_xml
    assert '<w:outlineLvl w:val="0"/>' in document_xml
    assert '<w:outlineLvl w:val="1"/>' in document_xml


def test_update_fields_on_open_is_enabled_in_settings(tmp_path):
    docx_path = tmp_path / "update-fields.docx"
    document = Document()

    node4_renderer._enable_update_fields_on_open(document)
    document.save(docx_path)

    settings_xml = _read_docx_member(docx_path, "word/settings.xml")

    assert "<w:updateFields w:val=\"true\"" in settings_xml


def test_refresh_docx_toc_returns_false_when_win32com_is_unavailable(monkeypatch, tmp_path):
    docx_path = tmp_path / "report.docx"
    Document().save(docx_path)

    monkeypatch.setattr(node4_renderer.platform, "system", lambda: "Windows")
    monkeypatch.setattr(
        node4_renderer,
        "get_settings",
        lambda: _FakeSettings(enable_auto_toc=True, auto_toc_backend="word_com"),
    )
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("win32com") or name == "pythoncom":
            raise ImportError(name)
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    assert node4_renderer.refresh_docx_toc(docx_path) is False


def test_refresh_docx_toc_returns_false_without_calling_backend_when_config_disabled(
    monkeypatch,
    tmp_path,
):
    docx_path = tmp_path / "report.docx"
    Document().save(docx_path)

    monkeypatch.setattr(
        node4_renderer,
        "get_settings",
        lambda: _FakeSettings(enable_auto_toc=False, auto_toc_backend="word_com"),
    )

    def fail_if_backend_called(_file_path):
        raise AssertionError("TOC backend should not be called when auto TOC is disabled")

    monkeypatch.setattr(node4_renderer, "_refresh_toc_with_word_com", fail_if_backend_called)

    assert node4_renderer.refresh_docx_toc(docx_path) is False


def test_refresh_docx_toc_returns_false_without_calling_backend_when_backend_none(
    monkeypatch,
    tmp_path,
):
    docx_path = tmp_path / "report.docx"
    Document().save(docx_path)

    monkeypatch.setattr(
        node4_renderer,
        "get_settings",
        lambda: _FakeSettings(enable_auto_toc=True, auto_toc_backend="none"),
    )

    def fail_if_backend_called(_file_path):
        raise AssertionError("TOC backend should not be called for AUTO_TOC_BACKEND=none")

    monkeypatch.setattr(node4_renderer, "_refresh_toc_with_word_com", fail_if_backend_called)

    assert node4_renderer.refresh_docx_toc(docx_path) is False


def test_refresh_docx_toc_libreoffice_backend_is_reserved(monkeypatch, tmp_path, caplog):
    docx_path = tmp_path / "report.docx"
    Document().save(docx_path)

    monkeypatch.setattr(
        node4_renderer,
        "get_settings",
        lambda: _FakeSettings(enable_auto_toc=True, auto_toc_backend="libreoffice"),
    )

    with caplog.at_level(logging.WARNING):
        assert node4_renderer.refresh_docx_toc(docx_path) is False

    assert "libreoffice backend 尚未实现" in caplog.text


def test_refresh_docx_toc_aspose_backend_is_reserved(monkeypatch, tmp_path, caplog):
    docx_path = tmp_path / "report.docx"
    Document().save(docx_path)

    monkeypatch.setattr(
        node4_renderer,
        "get_settings",
        lambda: _FakeSettings(enable_auto_toc=True, auto_toc_backend="aspose"),
    )

    with caplog.at_level(logging.WARNING):
        assert node4_renderer.refresh_docx_toc(docx_path) is False

    assert "aspose backend 尚未实现" in caplog.text
