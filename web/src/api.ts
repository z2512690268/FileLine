export type Experiment = {
  name: string;
  description: string;
  current: boolean;
  sourceMode: string;
  dataRoot: string;
  database: string;
  createdAt: string;
  stats: {
    total?: number;
    raw?: number;
    processed?: number;
    plots?: number;
    exports?: number;
    cacheRows?: number;
    versions?: number;
  };
};

export type Entry = {
  id: number;
  type: string;
  path: string;
  originalPath: string;
  fileName: string;
  extension: string;
  size: number | null;
  timestamp: string;
  description: string;
  tags: string[];
  parentIds: number[];
};

export type ExportItem = {
  name: string;
  entryId: number;
  createdAt: string;
  entry: Entry;
};

export type PipelineSummary = {
  path: string;
  name: string;
  description: string;
  group: string;
  stepCount: number;
  sourceCount: number;
  exportCount: number;
  processors: string[];
};

export type GraphNode = {
  id: string;
  type: "source" | "processor" | "export";
  label: string;
  subtitle: string;
  meta: Record<string, unknown>;
};

export type GraphEdge = {
  id: string;
  source: string;
  target: string;
  label: string;
};

export type PipelineDetail = PipelineSummary & {
  config: Record<string, unknown>;
  yaml: string;
  globals: Record<string, string>;
  graph: {
    nodes: GraphNode[];
    edges: GraphEdge[];
  };
};

export type Processor = {
  name: string;
  category: string;
  inputType: string;
  outputType: string;
  outputExt: string;
  hash: string;
  module: string;
  sourceFile: string;
  description: string;
  signature?: Array<{
    name: string;
    default: unknown;
    hasDefault: boolean;
  }>;
};

export type ProcessorTemplate = {
  id: string;
  name: string;
  description: string;
  filename: string;
  code: string;
};

export type ProcessorValidation = {
  ok: boolean;
  filename: string;
  errors: string[];
  warnings: string[];
  processors: Array<{
    name: string;
    function: string;
    inputType: string;
    outputType: string;
    outputExt: string;
    params: string[];
    errors: string[];
    warnings: string[];
  }>;
};

export type ProcessorSaveResult = {
  path: string;
  filename: string;
  validation: ProcessorValidation;
};

export type Version = {
  id: number;
  timestamp: string;
  configFile: string;
  pipelinePath?: string;
  restoredPipelinePath?: string;
  entryIds: number[];
  exportId: number | null;
  exportName: string;
  cacheScope?: string;
  hasConfigSnapshot?: boolean;
  status: string;
};

export type StorageVersion = {
  id: number;
  status: string;
  configFile: string;
  exportName: string;
  timestamp: string;
  entryCount: number;
  exclusiveProcessedEntries: number;
  reclaimableBytes: number;
  deletable: boolean;
};

export type StorageReport = {
  basePath: string;
  totalBytes: number;
  databaseBytes: number;
  byCategory: Record<string, number>;
  pipeline: {
    configFile: string;
    versionCount: number;
    activeVersionCount: number;
    reclaimableBytes: number;
  };
  versions: StorageVersion[];
};

export type DeleteVersionResult = {
  deleted: boolean;
  versionId: number;
  deletedEntries: number;
  deletedFiles: string[];
  freedBytes: number;
  keptEntries: number;
};

export type Preview =
  | { kind: "table"; columns: string[]; rows: Record<string, unknown>[]; shape: [number, number] }
  | { kind: "text"; text: string; truncated: boolean }
  | { kind: "file"; extension: string };

export type Lineage = {
  nodes: Array<Entry & { processor: string }>;
  edges: Array<{ id: string; source: number; target: number }>;
};

export type SourceCandidate = {
  entry: Entry;
  kind: "cached" | "local" | "remote";
  path: string;
  directory: string;
  pattern: string;
  label: string;
};

export type SourceSpec = {
  path: string;
  source?: string;
  remote?: string;
  regex?: string;
  sort_by?: string;
  sort_key?: string;
  limit?: number;
  tags?: string[];
  type?: string;
};

export type PipelineRunResult = {
  ok: boolean;
  returnCode: number;
  stdout: string;
  stderr: string;
  command: string[];
  forceFresh: boolean;
  summary: string;
  errorType: string;
  failedStage: string;
  failedProcessor: string;
  hint: string;
  sourceMode?: string;
};

export type StageResults = Record<string, Entry[]>;

export type ChartPlan = {
  supported: boolean;
  reason: string;
  chartType: string;
  processor: string | null;
  confidence: string;
  columns: Array<{
    name: string;
    kind: string;
    uniqueCount: number;
    sample: string[];
  }>;
  shape: [number, number];
  sampleRows: Record<string, unknown>[];
  params: Record<string, string | number | boolean | null>;
  title: string;
};

export type ChartSetup = {
  chartType?: string;
  title?: string;
  xCol?: string;
  yCol?: string;
  groupCol?: string;
};

async function request<T>(path: string): Promise<T> {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json() as Promise<T>;
}

async function mutate<T>(path: string): Promise<T> {
  const response = await fetch(path, { method: "POST" });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json() as Promise<T>;
}

async function remove<T>(path: string): Promise<T> {
  const response = await fetch(path, { method: "DELETE" });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json() as Promise<T>;
}

async function sendJson<T>(path: string, method: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json() as Promise<T>;
}

async function errorMessage(response: Response) {
  const text = await response.text();
  if (!text) return response.statusText || `Request failed (${response.status})`;
  try {
    const payload = JSON.parse(text) as { detail?: unknown; message?: string; error?: string };
    if (typeof payload.detail === "string") return payload.detail;
    if (payload.detail && typeof payload.detail === "object") {
      const detail = payload.detail as { message?: string; errors?: string[] };
      if (detail.message && Array.isArray(detail.errors) && detail.errors.length) {
        return `${detail.message}: ${detail.errors.join("; ")}`;
      }
      if (detail.message) return detail.message;
      return JSON.stringify(detail);
    }
    if (payload.message && payload.error) return `${payload.error}: ${payload.message}`;
    if (payload.message) return payload.message;
    if (payload.error) return payload.error;
  } catch {
    // Fall through to text response.
  }
  return text;
}

export const api = {
  experiments: () => request<Experiment[]>("/api/experiments"),
  entries: (experiment: string) => request<Entry[]>(`/api/experiments/${experiment}/entries`),
  sources: (experiment: string) => request<SourceCandidate[]>(`/api/experiments/${experiment}/sources`),
  uploadSource: async (experiment: string, file: File) => {
    const body = new FormData();
    body.append("file", file);
    const response = await fetch(`/api/experiments/${experiment}/sources/upload`, { method: "POST", body });
    if (!response.ok) {
      throw new Error(await errorMessage(response));
    }
    return response.json() as Promise<SourceCandidate>;
  },
  uploadProcessor: async (experiment: string, file: File) => {
    const body = new FormData();
    body.append("file", file);
    const response = await fetch(`/api/experiments/${experiment}/processors/upload`, { method: "POST", body });
    if (!response.ok) {
      throw new Error(await errorMessage(response));
    }
    return response.json() as Promise<ProcessorSaveResult>;
  },
  exports: (experiment: string) => request<ExportItem[]>(`/api/experiments/${experiment}/exports`),
  versions: (experiment: string, pipelinePath?: string) =>
    request<Version[]>(`/api/experiments/${experiment}/versions${pipelinePath ? `?pipeline_path=${encodeURIComponent(pipelinePath)}` : ""}`),
  storage: (experiment: string, pipelinePath?: string) =>
    request<StorageReport>(`/api/experiments/${experiment}/storage${pipelinePath ? `?pipeline_path=${encodeURIComponent(pipelinePath)}` : ""}`),
  switchVersion: (experiment: string, versionId: number, pipelinePath?: string) =>
    mutate<{ version: Version }>(`/api/experiments/${experiment}/versions/${versionId}/switch${pipelinePath ? `?pipeline_path=${encodeURIComponent(pipelinePath)}` : ""}`),
  deleteVersion: (experiment: string, versionId: number, pipelinePath?: string) =>
    remove<DeleteVersionResult>(`/api/experiments/${experiment}/versions/${versionId}${pipelinePath ? `?pipeline_path=${encodeURIComponent(pipelinePath)}` : ""}`),
  preview: (experiment: string, entryId: number) =>
    request<Preview>(`/api/experiments/${experiment}/entries/${entryId}/preview`),
  chartPlan: (experiment: string, entryId: number, goal: string) =>
    request<ChartPlan>(`/api/experiments/${experiment}/entries/${entryId}/chart-plan?goal=${encodeURIComponent(goal)}`),
  lineage: (experiment: string, entryId: number) =>
    request<Lineage>(`/api/experiments/${experiment}/entries/${entryId}/lineage`),
  fileUrl: (experiment: string, entryId: number) =>
    `/api/experiments/${experiment}/entries/${entryId}/file`,
  pipelines: () => request<PipelineSummary[]>("/api/pipelines"),
  pipeline: (path: string) => request<PipelineDetail>(`/api/pipelines/${path}`),
  clonePipeline: (sourcePath: string, name: string, experiment?: string, dataEntryId?: number, sourceSpec?: SourceSpec) =>
    sendJson<PipelineDetail>("/api/pipelines/clone", "POST", {
      source_path: sourcePath,
      name,
      experiment,
      data_entry_id: dataEntryId,
      source_spec: sourceSpec
    }),
  createAutoChart: (experiment: string, entryId: number, name: string, goal: string, setup?: ChartSetup) =>
    sendJson<PipelineDetail>(`/api/experiments/${experiment}/auto-chart`, "POST", {
      entry_id: entryId,
      name,
      goal,
      chart_type: setup?.chartType,
      title: setup?.title,
      x_col: setup?.xCol,
      y_col: setup?.yCol,
      group_col: setup?.groupCol
    }),
  stageResults: (experiment: string, path: string) =>
    request<StageResults>(`/api/experiments/${experiment}/pipelines/${path}/stage-results`),
  savePipeline: (path: string, yamlText: string) =>
    sendJson<PipelineDetail>(`/api/pipelines/${path}`, "PUT", { yaml_text: yamlText }),
  renamePipeline: (path: string, name: string, experiment?: string) =>
    sendJson<PipelineDetail>(`/api/pipelines/${path}/rename`, "POST", { name, experiment }),
  runPipeline: (experiment: string, path: string, dryRun: boolean, forceFresh = false, sourceMode = "") =>
    mutate<PipelineRunResult>(
      `/api/experiments/${experiment}/pipelines/${path}/run?dry_run=${dryRun ? "true" : "false"}&force_fresh=${forceFresh ? "true" : "false"}${sourceMode ? `&source_mode=${encodeURIComponent(sourceMode)}` : ""}`
    ),
  processors: (experiment?: string) => request<Processor[]>(`/api/processors${experiment ? `?experiment=${encodeURIComponent(experiment)}` : ""}`),
  processorTemplates: () => request<ProcessorTemplate[]>("/api/processors/templates"),
  validateProcessor: (experiment: string, filename: string, code: string) =>
    sendJson<ProcessorValidation>(`/api/experiments/${experiment}/processors/validate`, "POST", { filename, code }),
  saveProcessor: (experiment: string, filename: string, code: string) =>
    sendJson<ProcessorSaveResult>(`/api/experiments/${experiment}/processors/save`, "POST", { filename, code })
};
