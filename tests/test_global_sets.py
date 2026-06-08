from core import global_sets
from core.global_sets import global_set_affected, parse_global_set_text, resolve_text, save_global_set


def test_parse_yaml_global_set_values():
    text = """
name: paper
variables:
  PRIMARY_COLOR:
    type: color
    value: "#2563eb"
  FIG_SIZE:
    type: tuple
    value: [7, 4]
  LINE_COLORS:
    type: mapping
    value:
      A: "#2563eb"
      B: "#f97316"
"""
    values, meta = parse_global_set_text(text)
    assert meta["name"] == "paper"
    assert values["PRIMARY_COLOR"] == "#2563eb"
    assert values["FIG_SIZE"] == "[7, 4]"
    assert values["LINE_COLORS"].startswith("{A:")


def test_resolve_text_reports_used_and_missing_variables():
    result = resolve_text(
        "requires:\n  globals:\n    - FONT_FAMILY\ncolor: ${PRIMARY_COLOR}\nfigsize: ${FIG_SIZE}\nmissing: ${UNKNOWN}\n",
        {"PRIMARY_COLOR": "#2563eb", "FIG_SIZE": "[7, 4]"},
    )
    assert "color: #2563eb" in result["resolvedText"]
    assert "figsize: [7, 4]" in result["resolvedText"]
    assert result["used"] == {"FIG_SIZE": "[7, 4]", "PRIMARY_COLOR": "#2563eb"}
    assert result["missing"] == ["FONT_FAMILY", "UNKNOWN"]


def test_saving_experiment_set_does_not_overwrite_shared_set(tmp_path, monkeypatch):
    shared = tmp_path / "FileLine-Pipelines" / "_globals"
    experiment = tmp_path / "experiments" / "demo" / "globals"
    shared.mkdir(parents=True)
    experiment.mkdir(parents=True)
    (shared / "paper.yaml").write_text(
        "name: paper\nvariables:\n  PRIMARY_COLOR:\n    value: '#111111'\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(global_sets, "global_roots", lambda experiment_name=None: [shared, experiment])

    saved = save_global_set(
        "paper",
        "name: paper\nvariables:\n  PRIMARY_COLOR:\n    value: '#222222'\n",
        experiment="demo",
        scope="experiment",
    )

    assert saved["scope"] == "experiment"
    assert saved["values"]["PRIMARY_COLOR"] == "#222222"
    assert "#111111" in (shared / "paper.yaml").read_text(encoding="utf-8")


def test_affected_pipelines_include_bindings_and_outputs(tmp_path, monkeypatch):
    root = tmp_path / "FileLine-Pipelines"
    shared = root / "_globals"
    shared.mkdir(parents=True)
    (shared / "paper.yaml").write_text(
        "name: paper\nvariables:\n  PRIMARY_COLOR:\n    value: '#111111'\n",
        encoding="utf-8",
    )
    pipeline = root / "demo.yaml"
    pipeline.write_text(
        """
name: demo
global_set: paper
requires:
  globals:
    - PRIMARY_COLOR
initial_load:
  include: []
steps:
  - processor: plot_line
    output: figure
    params:
      color: ${PRIMARY_COLOR}
final_output:
  - name: figure
    export: figure.pdf
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(global_sets, "PIPELINES_ROOT", root)
    monkeypatch.setattr(global_sets, "global_roots", lambda experiment_name=None: [shared])

    affected = global_set_affected("paper")

    assert affected == [{
        "path": "demo.yaml",
        "variables": ["PRIMARY_COLOR"],
        "globalSet": "paper",
        "outputCount": 1,
        "outputs": ["figure.pdf"],
    }]
