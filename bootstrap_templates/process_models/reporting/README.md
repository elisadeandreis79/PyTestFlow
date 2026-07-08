# Reporting

Modules that transform aggregated `PyTestflowState` data into JSON or HTML
reports. Reporting flows typically run from the process model's `report`
callback, consuming `ptf_context.locals["main_result"]` to present sequence and
step outcomes captured by Prefect.

## Jinja HTML Reporter (opt-in)

`html_report.py` remains the default/legacy path.

To opt into Jinja-based rendering, use:

- `bootstrap_templates.process_models.reporting.html_jinja_report.generate_html_jinja_report`
- `bootstrap_templates.process_models.reporting.html_jinja_report.report_callback_jinja`

If `jinja2` is missing, only the Jinja reporter path raises a friendly runtime
error with install guidance. Existing `html_report.py` behavior is unchanged.

### Process model callback swap example

```python
from bootstrap_templates.process_models.sequential_model import SequentialProcessModel
from bootstrap_templates.process_models.reporting.html_jinja_report import report_callback_jinja

process_model = SequentialProcessModel(
    callbacks={
        "main_sequence": main_seq,
        "pre_uut": None,
        "post_uut": None,
        "report": report_callback_jinja,  # opt-in
        "database_logging": None,
    }
)
```

### Direct generation with template/CSS overrides

```python
from bootstrap_templates.process_models.reporting.html_jinja_report import generate_html_jinja_report

report_path = generate_html_jinja_report(
    serial_number="SN-001",
    root_state=main_results,
    template_path="my_template.html.j2",   # optional
    css_path="my_report.css",              # optional
)
```
