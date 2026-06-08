# FileLine

> Main Chinese documentation: [README.md](README.md)

FileLine is a reproducible data workflow and output studio for research experiments. It helps you turn raw experiment files into traceable final outputs, while keeping every source file, intermediate result, processor, pipeline version, and exported artifact connected.

Use it from either side:

- **Web UI** for browsing data, creating outputs, previewing lineage, comparing versions, and editing pipelines visually.
- **CLI** for scripted runs, batch processing, processor testing, version switching, and automation on remote machines.

Both surfaces share the same experiment directory, SQLite database, processor registry, pipeline runner, cache, and version records.

## Why FileLine

Research data workflows often fail in the unglamorous middle:

- raw logs, CSVs, parquet files, and custom formats accumulate quickly;
- plotting scripts are copied, edited, and forgotten;
- intermediate files are hard to connect back to their inputs;
- rerunning the same processor wastes time;
- changing a parameter can silently overwrite the result you trusted yesterday.

FileLine treats every processing step as a registered processor and every pipeline run as a recoverable version. The result is a workflow where you can answer:

- Which pipeline generated this output?
- Which raw files and intermediate results led to it?
- Which processor code and parameters were used?
- Can I preview an older version without losing the current one?
- Can I rerun safely from cached data or force a fresh run when needed?

## Main Features

- **Experiment isolation**: each experiment has its own data root, database, outputs, processors, and pipeline snapshots.
- **Web Studio**: Home, Create Figure, Pipelines, Data, Processors, and Storage views.
- **Output-centric Data view**: final outputs first, with source inputs and intermediate results folded below.
- **Lineage tracing**: jump from final output to source and back again.
- **Pipeline DAG support**: YAML pipelines can branch, merge, and export named outputs.
- **Processor registry**: add custom Python processors with a decorator.
- **Automatic caching**: skip repeated work when inputs, parameters, processor code, and cache scope match.
- **Versioned runs**: compare output versions and switch a pipeline back to a previous version.
- **Processor snapshots**: pipeline versions can restore the processor code used by that run.
- **Safe cleanup**: version cleanup protects active versions and shared data.
- **CLI/Web compatibility**: Web runs call the same `main.py pipeline run` path as the CLI.

## Repository Layout

```text
FileLine/
  api_server.py                 # FastAPI server for Web UI
  main.py                       # CLI entrypoint
  core/                         # storage, processing, pipeline, versions
  commands/                     # CLI command groups
  processes/                    # built-in processors
  FileLine-Pipelines/           # reusable pipeline YAMLs and templates
  experiments/                  # experiment-local snapshots and processors
  web/                          # React + Vite frontend
  scripts/                      # web deploy/status/stop helpers
  docs/                         # design notes and operational docs
```

## Install

Python dependencies:

```bash
cd FileLine
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Web dependencies:

```bash
cd FileLine/web
npm install
```

The deployment script also runs `npm install` automatically if `web/node_modules` is missing.

## Quick Start: Web UI

Start the production-style single-process web server:

```bash
cd FileLine
bash scripts/deploy_fileline_web.sh
```

Default URL:

```text
http://<server-ip>:8088
```

Override host or port:

```bash
FILELINE_WEB_HOST=0.0.0.0 FILELINE_WEB_PORT=8090 bash scripts/deploy_fileline_web.sh
```

Check or stop the server:

```bash
bash scripts/status_fileline_web.sh
bash scripts/stop_fileline_web.sh
```

Development mode:

```bash
cd FileLine
python -m uvicorn api_server:app --host 127.0.0.1 --port 8000

cd FileLine/web
npm run dev
```

Then open:

```text
http://localhost:5173
```

## Web UI Guide

### Home

Home is the starting dashboard for the selected experiment.

- `View outputs`: open final outputs in Data.
- `Run recommended pipeline`: continue from existing data.
- `Add data to start`: create a source-backed pipeline.
- `Recent outputs`: open recent exports directly.
- Suggested pipelines: quick entry points for reusable workflows.

### Create Figure

Create Figure is the guided path for new users.

1. **Choose source**: select registered data, upload a file, or add a source glob.
2. **What should FileLine make?**: choose a goal, inspect the recommended output, optionally expand `Adjust fields` or `Preview data`.
3. **Confirm and create**: name the result and create an editable pipeline.

Advanced source setup and processor upload are intentionally folded so a new user can start without writing code.

### Pipelines

The Pipelines page is the editable workflow view.

- Search and open existing experiment pipelines.
- Copy from standard templates.
- Read a story-style pipeline summary.
- Switch to DAG view for graph inspection.
- Edit stage parameters without opening raw YAML.
- Save pipeline changes.
- Run `Check flow` for dry-run validation.
- Run `Generate result` to produce outputs and record versions.
- Preview stage results in a larger modal when the right panel is too narrow.
- Open the final output in Data.

### Data

Data is the unified browser for files and outputs.

- **Final outputs**: exported results from pipelines.
- **Intermediate results**: processed files created by stages.
- **Source inputs**: raw files loaded into the experiment.
- Preview PDF, images, text, CSV, and parquet-like tables.
- Use lineage to jump from output to source/intermediate files.
- From source/intermediate files, jump back to the final output that used them.
- Browse output versions, preview previous/next versions, compare versions, and open the matching pipeline version.

### Processors

Processor Studio is for advanced users who need custom parsing or transformation logic.

- Start from a template.
- Validate FileLine processor format.
- Save the processor into the selected experiment.
- Refresh processor registry for pipeline use.

### Storage

Storage helps inspect disk usage and clean old versions.

- Storage summary is scoped to the selected experiment and pipeline.
- Version cleanup only deletes non-active versions.
- Shared entries and active versions are protected.
- Use this page for managed cleanup instead of manually deleting files.

## Quick Start: CLI

Create and use an experiment:

```bash
python main.py experiment create demo --description "Demo experiment"
python main.py experiment use demo
```

Add a data file:

```bash
python main.py data add data/metrics.csv --description "training metrics"
```

Inspect data:

```bash
python main.py data list-recent --limit 5
python main.py data show --type raw --limit 20
python main.py data trace --id 42 --depth 5
```

Dry-run a pipeline without creating data:

```bash
python main.py pipeline run FileLine-Pipelines/_templates/standard_line_chart.yaml --dry-run
```

Run a pipeline:

```bash
python main.py pipeline run FileLine-Pipelines/_templates/standard_line_chart.yaml
```

Run with versioned data when the pipeline has an active version:

```bash
python main.py pipeline run FileLine-Pipelines/fmrl/timeline.yaml --source-mode version
```

Force a fresh cache scope:

```bash
python main.py pipeline run FileLine-Pipelines/fmrl/timeline.yaml --fresh-scope
```

List and switch versions:

```bash
python main.py pipeline history --config-file fmrl/timeline.yaml
python main.py pipeline switch 12
python main.py pipeline prev --config-file fmrl/timeline.yaml
python main.py pipeline next --config-file fmrl/timeline.yaml
```

Run one processor directly for testing:

```bash
python main.py process run plot_line 17 -p time_col=step -p value_col=loss
```

## CLI Reference

Global options:

```bash
python main.py --experiment fmrl data list-recent
python main.py --confirm-exp pipeline run FileLine-Pipelines/fmrl/timeline.yaml
```

### experiment

```bash
python main.py experiment create NAME --description "..."
python main.py experiment use NAME
python main.py experiment list
python main.py experiment delete NAME
```

### data

```bash
python main.py data add FILE_PATH --description "..."
python main.py data show --id 1 --tag train --type raw --limit 20
python main.py data list-recent --limit 5
python main.py data list-between 2026-06-01 2026-06-09
python main.py data tag DATA_ID TAG
python main.py data trace --id FINAL_OUTPUT_ID --depth 5
python main.py data trace --id FINAL_OUTPUT_ID --export-dir ./sources-for-paper
python main.py data check
python main.py data check --fix
```

Safe delete:

```bash
python main.py data delete 123 -y
```

If the selected data is referenced by pipeline versions, exports, or downstream lineage, FileLine refuses even with `-y`. To intentionally delete referenced data:

```bash
python main.py data delete 123 \
  --force-referenced \
  --confirm-referenced "DELETE REFERENCED DATA"
```

Use this only when you understand that CLI/Web lineage, versions, or exports may lose those entries.

### pipeline

```bash
python main.py pipeline run CONFIG_FILE
python main.py pipeline run CONFIG_FILE --dry-run
python main.py pipeline run CONFIG_FILE --global-config GLOBAL_FILE
python main.py pipeline run CONFIG_FILE --source-mode version
python main.py pipeline run CONFIG_FILE --source-mode raw
python main.py pipeline run CONFIG_FILE --source-mode external
python main.py pipeline run CONFIG_FILE --fresh-scope
python main.py pipeline history --config-file PIPELINE_PATH
python main.py pipeline switch VERSION_ID
python main.py pipeline prev --config-file PIPELINE_PATH
python main.py pipeline next --config-file PIPELINE_PATH
```

Source modes:

- `version`: reuse the raw data saved with the current pipeline version when the pipeline snapshot is unchanged.
- `raw`: reuse registered raw data entries.
- `external` / `auto`: match or pull from the real source paths in YAML.

### process

```bash
python main.py process run PROCESSOR_NAME INPUT_IDS
python main.py process run PROCESSOR_NAME INPUT_IDS -p key=value -p another=value
```

`INPUT_IDS` can be a single ID or a comma-separated list, depending on the processor input type.

## Pipeline YAML

A pipeline has three main parts:

1. `initial_load`: how source files enter FileLine.
2. `steps`: processor stages and their parameters.
3. `final_output`: named outputs exported for users.

Minimal line output:

```yaml
name: Standard line chart
description: Plot one numeric metric over an ordered x-axis.

initial_load:
  include:
    - path: "data/*.csv"
      source: initial
      tags: [metrics]
  type: raw
  global_tags: [demo]

steps:
  - processor: plot_line
    inputs: initial
    output: line_output
    params:
      time_col: step
      value_col: loss
      title: Training Loss
      xlabel: Step
      ylabel: Loss
      figsize: [6, 4]
      grid: true
      dpi: 300

final_output:
  - name: line_output
    export: training_loss.pdf
```

Branching outputs:

```yaml
steps:
  - processor: split_metrics
    inputs: initial
    outputs:
      train: train_table
      eval: eval_table
    params: {}

  - processor: plot_line
    inputs: train_table
    output: train_plot
    params:
      time_col: step
      value_col: loss

  - processor: plot_line
    inputs: eval_table
    output: eval_plot
    params:
      time_col: step
      value_col: accuracy

final_output:
  - name: train_plot
    export: train_loss.pdf
  - name: eval_plot
    export: eval_accuracy.pdf
```

Standard templates live in:

```text
FileLine-Pipelines/_templates/
  standard_line_chart.yaml
  standard_multi_line_chart.yaml
  standard_bar_chart.yaml
  standard_grouped_bar_chart.yaml
  standard_horizontal_bar_chart.yaml
  standard_dual_axis_line_chart.yaml
  standard_timeline_chart.yaml
  standard_filter_then_plot.yaml
  standard_branch_compare.yaml
```

## Processor Development

Processors are regular Python functions registered with `ProcessorRegistry.register`.

Single-input example:

```python
from pathlib import Path
import pandas as pd
from core.processing import ProcessorRegistry, InputPath


@ProcessorRegistry.register(input_type="single", output_ext=".csv")
def normalize_loss(input_path: InputPath, output_path: Path, value_col: str = "loss"):
    df = pd.read_csv(input_path.path)
    df[value_col] = df[value_col] / df[value_col].max()
    df.to_csv(output_path, index=False)
    return f"normalized {value_col}"
```

Multi-input example:

```python
from pathlib import Path
import pandas as pd
from core.processing import ProcessorRegistry, InputPath


@ProcessorRegistry.register(input_type="multi", output_ext=".csv")
def concat_tables(inputs: list[InputPath], output_path: Path):
    frames = [pd.read_csv(item.path) for item in inputs]
    pd.concat(frames, ignore_index=True).to_csv(output_path, index=False)
    return "concatenated tables"
```

Where processors can live:

- Built-in processors: `processes/`
- Experiment-specific processors: `experiments/<experiment>/processors/`
- Pipeline-local processors: `<pipeline-directory>/processors/`

Processor Studio in the Web UI can create, validate, and save experiment processors without editing files manually.

## Caching and Versioning

FileLine caches processor outputs when these inputs match:

- input data IDs and file metadata;
- processor name and code hash;
- processor parameters;
- cache scope;
- pipeline version context.

A normal run reuses cached stage outputs when possible. Use `--fresh-scope` or the Web `Recompute processors` option when you intentionally want to bypass cached step outputs.

Each successful pipeline run records a `PipelineVersion` with:

- entry IDs produced by the run;
- exported output ID and export name;
- pipeline config snapshot;
- processor snapshot;
- result hash;
- active/superseded status;
- cache scope.

If the result hash, export name, config snapshot, and processor snapshot are unchanged, FileLine reactivates the matching version instead of creating duplicate version records.

## CLI and Web Compatibility

The Web UI runs pipelines by calling the same CLI path:

```bash
python main.py --experiment <name> pipeline run <pipeline.yaml>
```

This means:

- Web-generated outputs are visible to the CLI.
- CLI-generated versions and exports are visible in Data and Pipelines.
- Cache reuse and version recording behave the same from both surfaces.
- Processor snapshots and lineage are shared.

Known concurrency limitation:

- Concurrent editing of the same pipeline YAML is currently last-writer-wins.
- See `docs/concurrency-plan.md` for the planned revision and advisory-lock design.

## Operational Safety

Recommended habits:

- Use `pipeline run --dry-run` or Web `Check flow` before a new source pattern.
- Prefer Storage version cleanup over manual file deletion.
- Use `data trace --id <output>` before deleting source or intermediate data.
- Do not edit the SQLite database directly.
- Do not manually delete files under an experiment while Web or CLI runs are active.

Destructive operations with built-in protection:

- Active pipeline versions cannot be deleted by Storage cleanup.
- Version cleanup only removes entries exclusive to a deleted version.
- `data delete` refuses referenced entries unless the exact strong confirmation phrase is supplied.

## Testing

Run all tests:

```bash
pytest -q
```

Run CLI tests:

```bash
pytest -q tests/test_cli.py
```

Build the Web UI:

```bash
cd web
npm run build
```

## Troubleshooting

Check current experiment:

```bash
python main.py experiment list
```

Run a pipeline without writing files:

```bash
python main.py pipeline run path/to/pipeline.yaml --dry-run
```

Check DB and file consistency:

```bash
python main.py data check
python main.py data check --fix
```

Check Web server:

```bash
bash scripts/status_fileline_web.sh
tail -n 80 .run/fileline-web.log
```

If the Web UI shows stale data after a CLI run, refresh the experiment or page. Both surfaces share the same database, but the browser may still hold old local state.

## Recommended Tools

For local development, these tools make inspection easier:

- SQLite Viewer for experiment databases.
- Parquet Viewer or Parquet Visualizer for table outputs.
- A Python environment with pandas, matplotlib, SQLAlchemy, FastAPI, and Uvicorn.

## License and Status

FileLine is actively evolving as a research workflow and output studio. The stable contract today is the shared CLI/Web data model, pipeline runner, cache, version records, and processor registry.
