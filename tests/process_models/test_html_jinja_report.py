from pathlib import Path

import pytest

from bootstrap_templates.process_models.reporting import html_report
from bootstrap_templates.process_models.reporting.html_report import (
    generate_html_jinja_report,
    report_callback_jinja,
)
from bootstrap_templates.process_models.sequential_model import SequentialProcessModel
from pytestflow.backend.report_manager import report_manager
from pytestflow.core.context import ptf_context
from pytestflow.steps.action_step import action_step


class DummyState:
    def __init__(
        self,
        *,
        name: str,
        state_type: str = "COMPLETED",
        message: str = "",
        ptf_result: dict | None = None,
        children: list | None = None,
    ):
        self.name = name
        self.type = state_type
        self.message = message
        self.ptf_result = ptf_result or {}
        self.children = children or []
        self.task_run_id = None
        self.flow_run_id = None


@pytest.fixture(autouse=True)
def clear_state():
    ptf_context.locals.clear()
    ptf_context.globals.clear()
    report_manager._last_report = None
    yield
    ptf_context.locals.clear()
    ptf_context.globals.clear()
    report_manager._last_report = None


def _make_root_state() -> DummyState:
    child_pass = DummyState(
        name="passed",
        ptf_result={"step_name": "measure_voltage", "step_type": "numeric_limit", "value": 12.1},
    )
    child_fail = DummyState(
        name="failed",
        message="Threshold exceeded",
        ptf_result={"step_name": "check_current", "step_type": "pass_fail", "value": False},
    )
    root = DummyState(
        name="completed",
        children=[
            ("measure_voltage", child_pass),
            ("check_current", child_fail),
        ],
    )
    return root


def test_generate_html_jinja_report_default_template(tmp_path: Path):
    root_state = _make_root_state()

    report_path = generate_html_jinja_report("SN-001", root_state, out_dir=str(tmp_path))
    report_file = Path(report_path)

    assert report_file.exists()
    html_text = report_file.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in html_text
    assert "Test Sequence Report - SN: SN-001" in html_text
    assert "measure_voltage" in html_text


def test_generate_html_jinja_report_template_override(tmp_path: Path):
    root_state = _make_root_state()
    custom_template = tmp_path / "custom_report.html.j2"
    custom_template.write_text(
        "<html><body>CUSTOM_MARKER {{ title }} COUNT={{ summary_rows|length }}</body></html>",
        encoding="utf-8",
    )

    report_path = generate_html_jinja_report(
        "SN-OVERRIDE",
        root_state,
        out_dir=str(tmp_path),
        template_path=str(custom_template),
    )

    html_text = Path(report_path).read_text(encoding="utf-8")
    assert "CUSTOM_MARKER" in html_text
    assert "COUNT=2" in html_text


def test_generate_html_jinja_report_css_override(tmp_path: Path):
    root_state = _make_root_state()
    css_file = tmp_path / "custom.css"
    css_file.write_text(".from-css-path { color: red; }", encoding="utf-8")

    report_path = generate_html_jinja_report(
        "SN-CSS",
        root_state,
        out_dir=str(tmp_path),
        css_path=str(css_file),
    )

    html_text = Path(report_path).read_text(encoding="utf-8")
    assert ".from-css-path { color: red; }" in html_text


def test_generate_html_jinja_report_missing_jinja(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    root_state = _make_root_state()

    def _raise_missing():
        raise RuntimeError("Jinja reporting requires 'jinja2'. Install it with: pip install jinja2")

    monkeypatch.setattr(html_report, "_load_jinja2", _raise_missing)

    with pytest.raises(RuntimeError, match="pip install jinja2"):
        generate_html_jinja_report("SN-MISSING", root_state, out_dir=str(tmp_path))


def test_report_callback_jinja_sets_report_manager(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    @action_step(name="main_for_jinja_report_test")
    def main_for_jinja_report_test():
        return True

    monkeypatch.setattr(html_report, "REPORTS_FOLDER", str(tmp_path))

    model = SequentialProcessModel(
        name="pm_jinja_report",
        callbacks={
            "pre_uut": None,
            "main_sequence": main_for_jinja_report_test,
            "post_uut": None,
            "report": report_callback_jinja,
            "database_logging": None,
        },
    )

    model.run(return_state=True)
    report_path = report_manager.get_last_report()

    assert report_path is not None
    report_file = Path(report_path)
    assert report_file.exists()
    assert "<html" in report_file.read_text(encoding="utf-8").lower()

