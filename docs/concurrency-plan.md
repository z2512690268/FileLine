# FileLine CLI/Web Concurrency Plan

## Current State

The web app and CLI share the same core execution path for pipeline runs, data entries, cache records, exports, and pipeline versions. This keeps normal operations compatible.

The weak point is concurrent editing of mutable files and pointers:

- Pipeline YAML can be saved by the web app while a CLI/user edit is happening.
- Pipeline rename can race with save/run/open-version actions.
- Processor files can be edited while a run is starting.
- Active version pointers and exported files can be changed by CLI undo while the web UI still has stale state.

Today the last writer generally wins for YAML/file writes. That is predictable, but not a good product contract.

## Goals

- Prevent silent overwrites between web and CLI.
- Keep CLI scripts usable and non-interactive by default.
- Make conflicts explainable: who changed what, when, and how to recover.
- Avoid database-wide locks during long pipeline runs.
- Keep final outputs, Data lineage, and Pipeline versions consistent across both surfaces.

## Resource Model

Add revision metadata for mutable resources:

- `pipeline_yaml`: path, content hash, mtime, size, revision id.
- `processor_file`: path, content hash, mtime, size, revision id.
- `active_version`: pipeline path, active version id, revision id.
- `export_file`: export name, data entry id, content hash, revision id.

The first implementation can store this in a small JSON sidecar under the experiment directory:

`experiments/<name>/.fileline_state/revisions.json`

Later this can move into SQLite if needed.

## Save Contract

All write operations should become conditional writes:

- Web `GET pipeline` returns `revision`.
- Web `PUT pipeline` sends `baseRevision`.
- CLI `pipeline save` or rename reads current revision before writing.
- If current revision differs from `baseRevision`, reject with conflict.

Conflict response:

```json
{
  "error": "conflict",
  "resource": "pipeline",
  "path": "fmrl/timeline.yaml",
  "baseRevision": "abc",
  "currentRevision": "def",
  "message": "Pipeline changed since you opened it."
}
```

Web behavior:

- Show "Pipeline changed outside this tab".
- Offer Reload, View diff, Save as copy.

CLI behavior:

- Default: fail with a clear message.
- `--force` can overwrite intentionally.
- `--save-as <path>` can create a copy.

## Locking Contract

Use short-lived advisory locks only around writes, not around long runs:

- Pipeline save/rename: lock target YAML path.
- Processor save: lock processor file path.
- Active version switch/undo: lock active version pointer and export file update.
- Version cleanup/delete: lock version cleanup for the experiment.

Lock file format:

```json
{
  "owner": "web:session-id or cli:pid",
  "operation": "pipeline-save",
  "createdAt": "ISO timestamp",
  "expiresAt": "ISO timestamp"
}
```

Locks should have a short TTL, for example 30 seconds for saves and 2 minutes for cleanup. Stale locks can be broken with a warning.

## Pipeline Run Contract

Pipeline runs should snapshot inputs at start:

- Read pipeline YAML content and revision.
- Read processor source hashes.
- Record those snapshots in `PipelineVersion.config_snapshot` and `processor_snapshot`.
- The run may continue even if the YAML changes later, because the recorded version points to the exact content used.

If a user clicks Generate in web with stale YAML:

- If the UI has unsaved edits, require save first.
- If the server revision changed since load, block and show conflict.

CLI `pipeline run`:

- Runs the file as it exists at command start.
- Records the config snapshot, so web can display exactly what ran.

## Version and Export Safety

Active version changes should be atomic:

1. Start transaction.
2. Update active/superseded statuses.
3. Update export metadata.
4. Copy export file.
5. Stamp `.version`.
6. Commit or roll back.

If the file copy fails, the active pointer should not advance.

## Delete Safety

Destructive data deletion should reject referenced entries by default.

Already implemented policy:

- If selected DataEntry IDs are referenced by pipeline versions, exports, or downstream lineage, `data delete -y` still refuses.
- To override, the user must pass `--force-referenced` and confirm the exact phrase `DELETE REFERENCED DATA`.

Next step:

- Reuse the same reference check in any future web/API data delete endpoint.

## Implementation Phases

### Phase 1: Revision Read/Write

- Add revision helper for path hash + mtime.
- Return pipeline revision from `GET /api/pipelines/{path}`.
- Require optional `baseRevision` in pipeline save API.
- Add CLI save/rename revision checks where commands write pipeline YAML.

### Phase 2: Conflict UX

- Web shows reload/diff/save-copy when save conflicts.
- CLI prints current/base revision and suggested commands.

### Phase 3: Advisory Locks

- Add lock helper with TTL and stale lock cleanup.
- Wrap pipeline save, rename, processor save, version switch, cleanup.

### Phase 4: Atomic Version Switch

- Make version switch/export update transactional as far as possible.
- Write export to temporary path first, then rename into place.

### Phase 5: Audit Trail

- Record write events:
  - actor: web/cli
  - resource
  - old revision
  - new revision
  - timestamp

This makes "what changed my pipeline?" answerable from both surfaces.

## Acceptance Tests

- Two simulated clients load the same pipeline; first save succeeds, second save gets conflict.
- CLI rename while web has unsaved edits causes web save conflict.
- Pipeline run records original YAML snapshot even if YAML changes during run.
- Processor save during run does not change the processor snapshot for the already-started run.
- Version switch failure during export copy leaves active version unchanged.
- Data delete refuses referenced entries unless exact strong confirmation is provided.
