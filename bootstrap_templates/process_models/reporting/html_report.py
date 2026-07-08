from __future__ import annotations
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from pytestflow.backend.report_manager import report_manager
from pytestflow.config.config_manager import ConfigManager
from pytestflow.core.context import ptf_context
from pytestflow.steps.action_step import action_step


DEFAULT_TEMPLATE_PATH = Path(__file__).resolve().parent / "default_report.html.j2"
DEFAULT_CSS = """
:root { --fg:#222; --bg:#fff; --muted:#666; --border:#ddd; }
body { font-family: system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; color:var(--fg); background:var(--bg); margin:24px; line-height:1.4; }
h1 { margin:0 0 6px 0; }
h2 { margin-top: 20px; }
h3 { margin: 8px 0; }
.meta { color:var(--muted); margin-bottom:20px; }
.summary-table { width:100%; border-collapse: collapse; margin: 8px 0 18px 0; }
.summary-table th, .summary-table td { padding:8px 10px; border-bottom:1px solid var(--border); vertical-align: top; text-align: left; }
.summary-table th { background:#fafafa; }
.row { display:flex; gap:8px; align-items:center; flex-wrap: wrap; }
.indent { padding-left: 22px; border-left: 3px solid #f0f0f0; margin: 6px 0 10px 0; }
.badge { display:inline-block; padding:2px 8px; border-radius:10px; font-size:12px; font-weight:600; }
.badge-pass { background:#e8f8ee; color:#117a3a; border:1px solid #bfead0; }
.badge-fail { background:#fdeceb; color:#a1130f; border:1px solid #f3c2bf; }
.badge-other { background:#eef2ff; color:#283593; border:1px solid #c5cae9; }
code.inline { background:#f7f7f9; padding:2px 4px; border-radius:4px; }
ul.toc { margin: 8px 0 20px 0; }
ul.toc li { margin: 3px 0; }
"""


def _load_jinja2():
    try:
        import jinja2
    except Exception as exc:  # pragma: no cover - exercised via tests with monkeypatch
        raise RuntimeError(
            "Jinja reporting requires 'jinja2'. Install it with: pip install jinja2"
        ) from exc
    return jinja2


def _get_reports_folder() -> str:
    try:
        config = ConfigManager().get_config()
        return config["test_reports"]
    except Exception:
        return "test_reports"


REPORTS_FOLDER = _get_reports_folder()


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def _slug(text: str) -> str:
    text = re.sub(r"\s+", "-", str(text).strip())
    text = re.sub(r"[^a-zA-Z0-9\-_.]+", "", text)
    return text or "item"


def _extract_children(state: Any) -> list[tuple[str, Any]]:
    return list(getattr(state, "children", []) or [])


def _extract_message(state: Any) -> str:
    return getattr(state, "message", "") or ""


def _extract_status_name(state: Any) -> str:
    name = getattr(state, "name", None)
    if name:
        return str(name)
    return state.__class__.__name__


def _extract_type_label(state: Any) -> str:
    return str(getattr(state, "type", "")) or ""


def _guess_step_label(name: str, state: Any) -> str:
    ptf_result = getattr(state, "ptf_result", None)
    if isinstance(ptf_result, dict) and "step_name" in ptf_result:
        return str(ptf_result["step_name"])
    msg = _extract_message(state)
    if name.startswith("<pytestflow.core.sequence.Sequence"):
        match = re.search(r"Sequence\s+(.+?)\s+(passed|failed)", msg, flags=re.I)
        if match:
            return f"{match.group(1)} (subsequence)"
        return "Subsequence"
    return name


def _status_class(status: str) -> str:
    status_upper = status.upper()
    if "PAS" in status_upper:
        return "badge-pass"
    if "FAIL" in status_upper or status_upper == "ERROR":
        return "badge-fail"
    return "badge-other"


def _collect_step_stats(state: Any) -> dict[str, int]:
    stats = {"passed": 0, "failed": 0, "done": 0, "other": 0, "total": 0}
    for _, child_state in _extract_children(state):
        stats["total"] += 1
        status = _extract_status_name(child_state).lower()
        if "pass" in status:
            stats["passed"] += 1
        elif "fail" in status:
            stats["failed"] += 1
        elif "done" in status:
            stats["done"] += 1
        else:
            stats["other"] += 1
    return stats


def _extract_step_details(state: Any) -> dict[str, Any]:
    ptf_result = getattr(state, "ptf_result", None)
    if not isinstance(ptf_result, dict):
        return {}

    excluded = {
        "step_name",
        "flow_run_id",
        "flow_name",
        "task_run_id",
        "task_name",
        "additional_info",
    }
    details: dict[str, Any] = {}
    for key, value in ptf_result.items():
        if key in excluded:
            continue
        details[str(key)] = value
    return details


def _extract_meta(state: Any) -> dict[str, Any]:
    ptf_result = getattr(state, "ptf_result", None)
    task_run_id = getattr(state, "task_run_id", None)
    flow_run_id = getattr(state, "flow_run_id", None)
    has_children = bool(_extract_children(state))
    run_id_label = "Flow Run ID" if has_children else "Task Run ID"
    run_id_value = flow_run_id if has_children else task_run_id

    meta: dict[str, Any] = {
        "ptf_state_type": state.__class__.__name__,
        "prefect_state_type": _extract_type_label(state),
        "run_id_label": run_id_label,
        "run_id_value": run_id_value,
    }

    if isinstance(ptf_result, dict):
        step_type = ptf_result.get("step_type")
        if step_type is not None:
            meta["step_type"] = step_type
        additional_info = ptf_result.get("additional_info")
        if isinstance(additional_info, dict):
            meta["additional_info"] = additional_info
    return meta


def _build_tree_node(
    state: Any,
    *,
    name: str,
    index: str,
    root_anchor: str,
    depth: int,
) -> dict[str, Any]:
    label = _guess_step_label(name, state)
    anchor_id = _slug(f"{root_anchor}-{index}-{label}")
    status = _extract_status_name(state)
    children: list[dict[str, Any]] = []
    for child_idx, (child_name, child_state) in enumerate(_extract_children(state), start=1):
        child_index = f"{index}.{child_idx}" if index else str(child_idx)
        children.append(
            _build_tree_node(
                child_state,
                name=str(child_name),
                index=child_index,
                root_anchor=root_anchor,
                depth=depth + 1,
            )
        )

    return {
        "label": label,
        "index": index,
        "display_label": f"{index} - {label}" if index else label,
        "anchor_id": anchor_id,
        "status": status,
        "status_class": _status_class(status),
        "type_label": _extract_type_label(state),
        "message": _extract_message(state),
        "is_subsequence": bool(children),
        "meta": _extract_meta(state),
        "step_details": _extract_step_details(state),
        "children": children,
        "depth": depth,
    }


def _flatten_tree(node: dict[str, Any], out: list[dict[str, Any]]) -> None:
    out.append(node)
    for child in node["children"]:
        _flatten_tree(child, out)


def _extract_serial_number_from_context() -> str:
    user_response = ptf_context.locals.get("_pre_uut_user_response")
    if not isinstance(user_response, dict):
        return ""
    return str(user_response.get("text", "") or "")


def build_report_context(serial_number: str, root_state: Any) -> dict[str, Any]:
    generated_at = _now_ts()
    title = "Test Sequence Report" + (f" - SN: {serial_number}" if serial_number else "")
    root_anchor = _slug(f"root-{title}")

    root_node = _build_tree_node(
        root_state,
        name=title,
        index="0",
        root_anchor=root_anchor,
        depth=0,
    )
    details: list[dict[str, Any]] = []
    _flatten_tree(root_node, details)

    summary_rows = []
    for i, child in enumerate(root_node["children"], start=1):
        summary_rows.append(
            {
                "index": i,
                "label": child["label"],
                "anchor_id": child["anchor_id"],
                "status": child["status"],
                "status_class": child["status_class"],
                "type_label": child["type_label"],
                "is_subsequence": child["is_subsequence"],
            }
        )

    return {
        "title": title,
        "serial_number": serial_number,
        "generated_at": generated_at,
        "top_status": _extract_status_name(root_state),
        "top_status_class": _status_class(_extract_status_name(root_state)),
        "top_type": _extract_type_label(root_state),
        "top_message": _extract_message(root_state),
        "stats": _collect_step_stats(root_state),
        "summary_rows": summary_rows,
        "tree": root_node,
        "details": details,
    }


def _read_css_text(css_path: str | None) -> str:
    if not css_path:
        return DEFAULT_CSS
    with open(css_path, "r", encoding="utf-8") as file_obj:
        return file_obj.read()


def generate_html_jinja_report(
    serial_number: str,
    root_state: Any,
    out_dir: str | None = None,
    template_path: str | None = None,
    css_path: str | None = None,
) -> str:
    if out_dir is None:
        out_dir = REPORTS_FOLDER
    _ensure_dir(out_dir)

    context = build_report_context(serial_number, root_state)
    context["css_text"] = _read_css_text(css_path)

    jinja2 = _load_jinja2()
    if template_path:
        template_file = Path(template_path).resolve()
        environment = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(template_file.parent)),
            autoescape=jinja2.select_autoescape(["html", "xml"]),
        )
        template = environment.get_template(template_file.name)
    else:
        environment = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(DEFAULT_TEMPLATE_PATH.parent)),
            autoescape=jinja2.select_autoescape(["html", "xml"]),
        )
        template = environment.get_template(DEFAULT_TEMPLATE_PATH.name)

    html_doc = template.render(**context)
    filename = f"{context['generated_at']}_{_slug(context['title'])}.html"
    out_path = os.path.join(out_dir, filename)
    with open(out_path, "w", encoding="utf-8") as file_obj:
        file_obj.write(html_doc)
    return out_path


@action_step(name="report_callback_jinja")
def report_callback_jinja():
    main_results = ptf_context.locals.get("main_results") or ptf_context.locals.get("main_result")
    if main_results is None:
        raise ValueError("Main sequence results are missing. Expected 'main_results' in context.")

    serial_number = _extract_serial_number_from_context()
    
    try:
        report_path = generate_html_jinja_report(serial_number, main_results)
        print(f"HTML Jinja report generated: {report_path}")
    except Exception as e:
        print(f"Jinja report generation failed: {e}.")
    
    report_manager.set_last_report(report_path)
    return report_path
