from core.global_sets import parse_global_set_text, resolve_text


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
        "color: ${PRIMARY_COLOR}\nfigsize: ${FIG_SIZE}\nmissing: ${UNKNOWN}\n",
        {"PRIMARY_COLOR": "#2563eb", "FIG_SIZE": "[7, 4]"},
    )
    assert "color: #2563eb" in result["resolvedText"]
    assert "figsize: [7, 4]" in result["resolvedText"]
    assert result["used"] == {"FIG_SIZE": "[7, 4]", "PRIMARY_COLOR": "#2563eb"}
    assert result["missing"] == ["UNKNOWN"]
