import { ReactNode, useEffect, useMemo, useCallback, useState, useRef } from "react";
import {
  Background,
  Controls,
  Edge,
  Handle,
  MiniMap,
  Node,
  Position,
  ReactFlow
} from "@xyflow/react";
import {
  Activity,
  ArrowRight,
  Boxes,
  ChartNoAxesCombined,
  CheckCircle,
  ChevronLeft,
  ChevronRight,
  Database,
  Download,
  Eye,
  FileBarChart,
  FileCode,
  GitBranch,
  HardDrive,
  Image as ImageIcon,
  Layers,
  Maximize2,
  Network,
  Play,
  RefreshCw,
  Search,
  SlidersHorizontal,
  Sparkles,
  Table2,
  Trash2,
  Upload,
  Wand2,
  X
} from "lucide-react";
import yaml from "js-yaml";
import {
  api,
  ChartPlan,
  ChartSetup,
  Entry,
  Experiment,
  ExportItem,
  GraphNode,
  Lineage,
  PipelineDetail,
  PipelineRunResult,
  PipelineSummary,
  Preview,
  Processor,
  ProcessorTemplate,
  ProcessorValidation,
  SourceCandidate,
  SourceSpec,
  StorageReport,
  StageResults,
  Version
} from "./api";

type View = "home" | "create" | "workbench" | "data" | "processorStudio" | "processors";

const nodeTypes = {
  fileline: FileLineNode
};

function normalizeView(value: string | null): View {
  if (value === "exports") return "data";
  if (value === "home" || value === "create" || value === "workbench" || value === "data" || value === "processorStudio" || value === "processors") return value;
  return "home";
}

type YamlStep = Record<string, unknown>;
type YamlConfig = Record<string, unknown>;

function asInputArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((item) => String(item).trim()).filter(Boolean);
  const text = String(value ?? "initial").trim();
  return text ? [text] : ["initial"];
}

function normalizeInputs(values: string[]): string | string[] {
  const deduped = [...new Set(values.map((value) => value.trim()).filter(Boolean))];
  if (deduped.length <= 1) return deduped[0] || "initial";
  return deduped;
}

function stepOutputVars(step: YamlStep | undefined, fallbackIndex: number): string[] {
  if (!step) return [`step_${fallbackIndex}`];
  const outputs = step.outputs as Record<string, unknown> | undefined;
  if (outputs && typeof outputs === "object") {
    return Object.values(outputs).map((value) => String(value).trim()).filter(Boolean);
  }
  return [String(step.output || `step_${fallbackIndex}`).trim()].filter(Boolean);
}

function allOutputVars(steps: YamlStep[]): string[] {
  return steps.flatMap((step, index) => stepOutputVars(step, index + 1));
}

function uniqueOutputName(steps: YamlStep[], base = "identity"): string {
  const used = new Set(allOutputVars(steps));
  let index = steps.length + 1;
  let name = `${base}_${index}`;
  while (used.has(name)) {
    index += 1;
    name = `${base}_${index}`;
  }
  return name;
}

function replaceInputRefs(value: unknown, targets: Set<string>, replacement: unknown): unknown {
  const replacementVars = asInputArray(replacement);
  if (Array.isArray(value)) {
    const next: string[] = [];
    for (const item of value) {
      const text = String(item);
      if (targets.has(text)) next.push(...replacementVars);
      else next.push(text);
    }
    return normalizeInputs(next);
  }
  const text = String(value ?? "");
  if (targets.has(text)) return normalizeInputs(replacementVars);
  return value;
}

function replaceFinalRefs(finals: YamlStep[], targets: Set<string>, replacement: unknown) {
  const replacementVars = asInputArray(replacement);
  const firstReplacement = replacementVars[0] || "initial";
  for (const finalOutput of finals) {
    const name = String(finalOutput.name ?? "");
    if (targets.has(name)) finalOutput.name = firstReplacement;
  }
}

function defaultParamsForProcessor(processors: Processor[] | undefined, name: string): Record<string, unknown> {
  const signature = processors?.find((processor) => processor.name === name)?.signature || [];
  const params: Record<string, unknown> = {};
  for (const param of signature) {
    if (param.name === "kwargs") continue;
    if (param.hasDefault && param.default !== undefined && param.default !== null) {
      params[param.name] = param.default;
    }
  }
  return params;
}

function processorGroup(processor: Processor) {
  const haystack = `${processor.category} ${processor.name} ${processor.description} ${processor.outputExt}`.toLowerCase();
  if (["plot", "chart", "figure", ".pdf", ".png", "bar", "line", "timeline"].some((token) => haystack.includes(token))) return "Plot";
  if (["table", "csv", "parquet", "filter", "merge", "group", "column", "concat"].some((token) => haystack.includes(token))) return "Table";
  if (["parse", "extract", "log", "timeline"].some((token) => haystack.includes(token))) return "Parse";
  if (["file", "identity", "copy"].some((token) => haystack.includes(token))) return "File";
  if (processor.sourceFile.includes("/experiments/") || processor.sourceFile.includes("FileLine-Pipelines")) return "Custom";
  return processor.category || "Other";
}

function processorSummary(processor: Processor | undefined) {
  if (!processor) return "";
  if (processor.description && processor.description !== processor.name) return processor.description;
  const name = processor.name.replace(/_/g, " ");
  if (processorGroup(processor) === "Plot") return `Draw a ${name} figure from prepared data.`;
  if (processorGroup(processor) === "Table") return `Transform tabular data with ${name}.`;
  if (processorGroup(processor) === "Parse") return `Parse source data into a cleaner table.`;
  if (processor.name === "file_identity") return "Pass data through unchanged; useful as a safe placeholder step.";
  return name;
}

function isExperimentPipeline(pipeline: PipelineSummary, experiment: string) {
  if (!experiment) return false;
  return pipeline.group === experiment || pipeline.path.startsWith(`${experiment}/`);
}

function isStandardTemplate(pipeline: PipelineSummary) {
  return pipeline.path.startsWith("_templates/");
}

function standardTemplatePipelines(pipelines: PipelineSummary[]) {
  return pipelines.filter(isStandardTemplate).sort((a, b) => a.name.localeCompare(b.name));
}

function currentExperimentPipelines(pipelines: PipelineSummary[], experiment: string) {
  const scoped = pipelines.filter((pipeline) => isExperimentPipeline(pipeline, experiment) && !isStandardTemplate(pipeline));
  return scoped.length ? scoped : pipelines.filter((pipeline) => pipeline.group === "root" && !isStandardTemplate(pipeline));
}

function pipelineGoalScore(pipeline: PipelineSummary, goal: string) {
  const text = `${pipeline.name} ${pipeline.path} ${pipeline.processors.join(" ")}`.toLowerCase();
  if (goal === "timeline") return text.includes("timeline") ? 4 : text.includes("cdf") ? 2 : 0;
  if (goal === "trend") return text.includes("line") || text.includes("curve") || text.includes("convergence") ? 4 : 0;
  if (goal === "breakdown") return text.includes("breakdown") || text.includes("bar") || text.includes("utilization") ? 4 : 0;
  return text.includes("throughput") || text.includes("scalability") || text.includes("bar") || pipeline.exportCount > 0 ? 3 : 1;
}

function curatedPipelines(pipelines: PipelineSummary[], experiment: string, goal: string) {
  return currentExperimentPipelines(pipelines, experiment)
    .map((pipeline) => ({ pipeline, score: pipelineGoalScore(pipeline, goal) }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score || b.pipeline.exportCount - a.pipeline.exportCount || a.pipeline.name.localeCompare(b.pipeline.name))
    .slice(0, 3)
    .map((item) => item.pipeline);
}

function dataFitScore(entry: Entry) {
  const ext = entry.extension.toLowerCase();
  let score = entry.type === "raw" ? 8 : 0;
  if ([".csv", ".tsv", ".parquet", ".json", ".jsonl", ".log", ".txt"].includes(ext)) score += 4;
  if (entry.tags.some((tag) => /timeline|metric|throughput|raw|input|log|table/i.test(tag))) score += 2;
  if ([".pdf", ".png", ".jpg", ".jpeg", ".svg"].includes(ext)) score -= 6;
  return score;
}

function FileLineNode({ data }: { data: GraphNode }) {
  const icon =
    data.type === "source" ? <Database size={16} /> : data.type === "export" ? <Download size={16} /> : <SlidersHorizontal size={16} />;
  return (
    <div className={`dag-node dag-node-${data.type}`}>
      <Handle type="target" position={Position.Left} />
      <div className="dag-node-title">
        {icon}
        <span>{data.label}</span>
      </div>
      <div className="dag-node-subtitle">{data.subtitle}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

function formatBytes(size: number | null) {
  if (size == null) return "-";
  const units = ["B", "KB", "MB", "GB"];
  let value = size;
  let index = 0;
  while (value > 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(value > 10 ? 0 : 1)} ${units[index]}`;
}

function shortDate(value: string) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function parseTimestamp(value: string) {
  const ts = Date.parse(value);
  return Number.isNaN(ts) ? 0 : ts;
}

function latestFinalEntry(
  pipeline: PipelineDetail | null,
  stageResults: StageResults,
  exports: ExportItem[]
) {
  if (!pipeline) return null;
  const exportNodes = pipeline.graph.nodes.filter((node) => node.type === "export");
  const finalCandidates = exportNodes.flatMap((node) => stageResults[node.id] || []);
  if (finalCandidates.length > 0) {
    return [...finalCandidates].sort((a, b) => parseTimestamp(b.timestamp) - parseTimestamp(a.timestamp) || b.id - a.id)[0];
  }
  return null;
}

function sourceSummaryFromNodes(nodes: GraphNode[]) {
  const sources = nodes.filter((node) => node.type === "source");
  if (!sources.length) return "Source: current experiment data";
  const labels = sources.map((node) => {
    const includes = ((node.meta || {}).includes as Array<Record<string, unknown>> | undefined) || [];
    const firstInclude = includes[0] || {};
    const path = String(firstInclude.path || firstInclude.regex || firstInclude.remote || node.label || "input");
    const suffix = includes.length > 1 ? ` +${includes.length - 1}` : "";
    return `${path}${suffix}`;
  });
  if (labels.length === 1) return `Source: ${labels[0]}`;
  return `Sources: ${labels.slice(0, 2).join(", ")}${labels.length > 2 ? ` +${labels.length - 2}` : ""}`;
}

export function App() {
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [pipelines, setPipelines] = useState<PipelineSummary[]>([]);
  const [processors, setProcessors] = useState<Processor[]>([]);
  const [selectedExperiment, setSelectedExperiment] = useState(() => localStorage.getItem("fl_exp") || "");
  const [selectedPipelinePath, setSelectedPipelinePath] = useState(() => localStorage.getItem("fl_pipe") || "");
  const [pipeline, setPipeline] = useState<PipelineDetail | null>(null);
  const [yamlDraft, setYamlDraft] = useState("");
  const [yamlDirty, setYamlDirty] = useState(false);
  const [saveBusy, setSaveBusy] = useState(false);
  const [entries, setEntries] = useState<Entry[]>([]);
  const [sources, setSources] = useState<SourceCandidate[]>([]);
  const [exports, setExports] = useState<ExportItem[]>([]);
  const [versions, setVersions] = useState<Version[]>([]);
  const [allVersions, setAllVersions] = useState<Version[]>([]);
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const [selectedEntry, setSelectedEntry] = useState<Entry | null>(null);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [lineage, setLineage] = useState<Lineage | null>(null);
  const [runResult, setRunResult] = useState<PipelineRunResult | null>(null);
  const [stageResults, setStageResults] = useState<StageResults>({});
  const [runBusy, setRunBusy] = useState(false);
  const [view, setView] = useState<View>(() => normalizeView(localStorage.getItem("fl_view")));
  const [focusedVersionId, setFocusedVersionId] = useState<number | null>(null);
  const [error, setError] = useState("");
  const pendingPipelinePathRef = useRef("");

  // Note: comment text was normalized to avoid mojibake.
  useEffect(() => { localStorage.setItem("fl_exp", selectedExperiment); }, [selectedExperiment]);
  useEffect(() => { localStorage.setItem("fl_pipe", selectedPipelinePath); }, [selectedPipelinePath]);
  useEffect(() => { localStorage.setItem("fl_view", view); }, [view]);

  async function refreshBase() {
    setError("");
    try {
      const [expData, pipeData] = await Promise.all([
        api.experiments(),
        api.pipelines()
      ]);
      setExperiments(expData);
      setPipelines(pipeData);
      const expNames = expData.map((e) => e.name);
      if (!selectedExperiment || !expNames.includes(selectedExperiment)) {
        setSelectedExperiment(expData.find((exp) => exp.current)?.name || expData[0]?.name || "");
      }
      if (!selectedPipelinePath) {
        const pref = pipeData.find((item) => item.path.includes("dag_demo")) || pipeData[0];
        setSelectedPipelinePath(pref?.path || "");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  useEffect(() => {
    refreshBase();
  }, []);

  useEffect(() => {
    if (!selectedPipelinePath) return;
    let cancelled = false;
    setPipeline(null);
    setSelectedNode(null);
    setRunResult(null);
    setError("");
    setStageResults({});
    api.pipeline(selectedPipelinePath).then((detail) => {
      if (cancelled) return;
      setPipeline(detail);
      setYamlDraft(detail.yaml);
      setYamlDirty(false);
      setSelectedNode(detail.graph.nodes[0] || null);
    }).catch((err) => setError(err instanceof Error ? err.message : String(err)));
    return () => {
      cancelled = true;
    };
  }, [selectedPipelinePath]);

  useEffect(() => {
    if (!selectedExperiment || !selectedPipelinePath) {
      setStageResults({});
      setVersions([]);
      setAllVersions([]);
      setRunResult(null);
      return;
    }
    let cancelled = false;
    setStageResults({});
    Promise.all([
      api.stageResults(selectedExperiment, selectedPipelinePath),
      api.versions(selectedExperiment, selectedPipelinePath)
    ])
      .then(([stageData, versionData]) => {
        if (!cancelled) {
          setStageResults(stageData);
          setVersions(versionData);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setStageResults({});
          setVersions([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedExperiment, selectedPipelinePath]);

  useEffect(() => {
    if (!selectedExperiment) return;
    let cancelled = false;
    setEntries([]);
    setSources([]);
    setExports([]);
    setVersions([]);
    setAllVersions([]);
    setProcessors([]);
    setSelectedEntry(null);
    setSelectedNode(null);
    setPreview(null);
    setLineage(null);
    setStageResults({});
    setRunResult(null);
    Promise.all([
      api.entries(selectedExperiment),
      api.sources(selectedExperiment),
      api.exports(selectedExperiment),
      api.versions(selectedExperiment),
      api.processors(selectedExperiment)
    ]).then(([entryData, sourceData, exportData, versionData, processorData]) => {
      if (cancelled) return;
      setEntries(entryData);
      setSources(sourceData);
      setExports(exportData);
      setAllVersions(versionData);
      setProcessors(processorData);
      setSelectedEntry(exportData[0]?.entry || entryData[0] || null);
    }).catch((err) => setError(err instanceof Error ? err.message : String(err)));
    return () => {
      cancelled = true;
    };
  }, [selectedExperiment]);

  useEffect(() => {
    if (!selectedExperiment || !selectedEntry) {
      setPreview(null);
      return;
    }
    let cancelled = false;
    setPreview(null);
    api.preview(selectedExperiment, selectedEntry.id)
      .then((data) => {
        if (!cancelled) setPreview(data);
      })
      .catch(() => setPreview(null));
    return () => {
      cancelled = true;
    };
  }, [selectedExperiment, selectedEntry]);

  useEffect(() => {
    if (!selectedExperiment || !selectedEntry) {
      setLineage(null);
      return;
    }
    let cancelled = false;
    setLineage(null);
    api.lineage(selectedExperiment, selectedEntry.id)
      .then((data) => {
        if (!cancelled) setLineage(data);
      })
      .catch(() => setLineage(null));
    return () => {
      cancelled = true;
    };
  }, [selectedExperiment, selectedEntry]);

  async function runSelectedPipeline(dryRun: boolean, forceFresh = false, sourceMode = "") {
    if (!selectedExperiment || !selectedPipelinePath) return;
    setRunBusy(true);
    setRunResult(null);
    setError("");
    try {
      const result = await api.runPipeline(selectedExperiment, selectedPipelinePath, dryRun, forceFresh, sourceMode);
      setRunResult(result);
      if (!dryRun && result.ok) {
        const [entryData, exportData, versionData, allVersionData] = await Promise.all([
          api.entries(selectedExperiment),
          api.exports(selectedExperiment),
          api.versions(selectedExperiment, selectedPipelinePath),
          api.versions(selectedExperiment)
        ]);
        setEntries(entryData);
        setExports(exportData);
        setVersions(versionData);
        setAllVersions(allVersionData);
        const stageData = await api.stageResults(selectedExperiment, selectedPipelinePath);
        setStageResults(stageData);
        const syncedFinal = latestFinalEntry(pipeline, stageData, exportData);
        if (syncedFinal) {
          setSelectedEntry(syncedFinal);
          const exportNode = pipeline?.graph.nodes.find((node) => node.type === "export" && (stageData[node.id] || []).some((entry) => entry.id === syncedFinal.id));
          if (exportNode) {
            setSelectedNode(exportNode);
          }
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setRunBusy(false);
    }
  }

  async function saveSelectedPipeline() {
    if (!selectedPipelinePath || !yamlDirty) return;
    setSaveBusy(true);
    setError("");
    try {
      const detail = await api.savePipeline(selectedPipelinePath, yamlDraft);
      setPipeline(detail);
      setYamlDraft(detail.yaml);
      setYamlDirty(false);
      setSelectedNode(detail.graph.nodes[0] || null);
      const pipeData = await api.pipelines();
      setPipelines(pipeData);
      if (selectedExperiment) {
        const [stageData, exportData, versionData, allVersionData] = await Promise.all([
          api.stageResults(selectedExperiment, selectedPipelinePath),
          api.exports(selectedExperiment),
          api.versions(selectedExperiment, selectedPipelinePath),
          api.versions(selectedExperiment)
        ]);
        setStageResults(stageData);
        setExports(exportData);
        setVersions(versionData);
        setAllVersions(allVersionData);
        const syncedFinal = latestFinalEntry(detail, stageData, exportData);
        if (syncedFinal) {
          setSelectedEntry(syncedFinal);
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaveBusy(false);
    }
  }

  async function renameSelectedPipeline(name: string) {
    if (!selectedPipelinePath) return;
    setSaveBusy(true);
    setError("");
    try {
      const detail = await api.renamePipeline(selectedPipelinePath, name, selectedExperiment || undefined);
      const newPath = detail.path;
      pendingPipelinePathRef.current = newPath;
      setPipeline(detail);
      setYamlDraft(detail.yaml);
      setYamlDirty(false);
      setSelectedNode(detail.graph.nodes[0] || null);
      const pipeData = await api.pipelines();
      setPipelines(pipeData.some((item) => item.path === newPath) ? pipeData : [detail, ...pipeData]);
      setSelectedPipelinePath(newPath);
      pendingPipelinePathRef.current = "";
      if (selectedExperiment) {
        const [stageData, versionData, allVersionData] = await Promise.all([
          api.stageResults(selectedExperiment, newPath),
          api.versions(selectedExperiment, newPath),
          api.versions(selectedExperiment)
        ]);
        setStageResults(stageData);
        setVersions(versionData);
        setAllVersions(allVersionData);
      }
    } catch (err) {
      pendingPipelinePathRef.current = "";
      setError(err instanceof Error ? err.message : String(err));
      throw err;
    } finally {
      setSaveBusy(false);
    }
  }

  async function switchToVersion(version: Version) {
    if (!selectedExperiment || version.status === "active") return;
    setRunBusy(true);
    setError("");
    try {
      const switched = await api.switchVersion(selectedExperiment, version.id, selectedPipelinePath || undefined);
      const [entryData, exportData, versionData, allVersionData] = await Promise.all([
        api.entries(selectedExperiment),
        api.exports(selectedExperiment),
        api.versions(selectedExperiment, selectedPipelinePath || undefined),
        api.versions(selectedExperiment)
      ]);
      setEntries(entryData);
      setExports(exportData);
      setVersions(versionData);
      setAllVersions(allVersionData);
      const restoredPath = switched.version.restoredPipelinePath || switched.version.pipelinePath || switched.version.configFile;
      if (restoredPath && restoredPath.includes("/") && restoredPath !== selectedPipelinePath && pipelines.some((item) => item.path === restoredPath)) {
        setSelectedPipelinePath(restoredPath);
      } else if (restoredPath && selectedPipelinePath) {
        const detail = await api.pipeline(selectedPipelinePath);
        setPipeline(detail);
        setYamlDraft(detail.yaml);
        setYamlDirty(false);
        setSelectedNode(detail.graph.nodes[0] || null);
      }
      if (selectedPipelinePath) {
        const stageData = await api.stageResults(selectedExperiment, selectedPipelinePath);
        setStageResults(stageData);
      }
      const exportId = switched.version.exportId;
      const restoredEntry = exportId ? entryData.find((entry) => entry.id === exportId) : null;
      if (restoredEntry) {
        setSelectedEntry(restoredEntry);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setRunBusy(false);
    }
  }

  function pipelinePathForVersion(version: Version) {
    const candidates = [version.restoredPipelinePath, version.pipelinePath, version.configFile].filter(Boolean) as string[];
    for (const candidate of candidates) {
      if (pipelines.some((item) => item.path === candidate)) return candidate;
    }
    const matched = pipelines.find((item) => candidates.some((candidate) => item.path.endsWith(`/${candidate}`)));
    return matched?.path || candidates[0] || selectedPipelinePath;
  }

  function openPipelineVersion(version: Version) {
    const path = pipelinePathForVersion(version);
    if (path) {
      setSelectedPipelinePath(path);
    }
    setFocusedVersionId(version.id);
    setView("workbench");
  }

  async function switchAndOpenPipelineVersion(version: Version) {
    const path = pipelinePathForVersion(version);
    if (path) {
      setSelectedPipelinePath(path);
    }
    setFocusedVersionId(version.id);
    setView("workbench");
    if (version.status !== "active") {
      await switchToVersion(version);
      const restoredPath = pipelinePathForVersion(version);
      if (restoredPath) {
        setSelectedPipelinePath(restoredPath);
      }
      setFocusedVersionId(version.id);
      setView("workbench");
    }
  }

  const groupedPipelines = useMemo(() => {
    const result = new Map<string, PipelineSummary[]>();
    for (const pipelineItem of currentExperimentPipelines(pipelines, selectedExperiment)) {
      result.set(pipelineItem.group, [...(result.get(pipelineItem.group) || []), pipelineItem]);
    }
    return [...result.entries()].sort(([a], [b]) => a.localeCompare(b));
  }, [pipelines, selectedExperiment]);

  useEffect(() => {
    if (!selectedExperiment || pipelines.length === 0) return;
    const visible = currentExperimentPipelines(pipelines, selectedExperiment);
    if (!visible.length) return;
    if (!selectedPipelinePath || !visible.some((pipelineItem) => pipelineItem.path === selectedPipelinePath)) {
      if (pendingPipelinePathRef.current && selectedPipelinePath === pendingPipelinePathRef.current) return;
      const preferred = visible.find((item) => item.name.includes("timeline") || item.name.includes("throughput")) || visible[0];
      setSelectedPipelinePath(preferred.path);
    }
  }, [selectedExperiment, pipelines, selectedPipelinePath]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark"><GitBranch size={20} /></div>
          <div>
            <h1>FileLine</h1>
            <p>Figure studio</p>
          </div>
        </div>

        <label className="field-label">Experiment</label>
        <select value={selectedExperiment} onChange={(event) => setSelectedExperiment(event.target.value)}>
          {experiments.map((experiment) => (
            <option key={experiment.name} value={experiment.name}>{experiment.name}</option>
          ))}
        </select>

        <nav className="nav-list">
          <NavButton active={view === "home"} icon={<Sparkles size={17} />} label="Home" onClick={() => setView("home")} />
          <NavButton active={view === "create"} icon={<Wand2 size={17} />} label="Create Figure" onClick={() => setView("create")} />
          <NavButton active={view === "workbench"} icon={<GitBranch size={17} />} label="Pipelines" onClick={() => setView("workbench")} />
          <NavButton active={view === "data"} icon={<Table2 size={17} />} label="Data" onClick={() => setView("data")} />
          <NavButton active={view === "processorStudio"} icon={<FileCode size={17} />} label="Processors" onClick={() => setView("processorStudio")} />
          <NavButton active={view === "processors"} icon={<Boxes size={17} />} label="Storage" onClick={() => setView("processors")} />
        </nav>

        <button className="ghost-button" onClick={refreshBase}>
          <RefreshCw size={15} />
          Refresh
        </button>
      </aside>

      <main className="main">
        <header className="topbar">
          <div>
            <p className="eyebrow">{selectedExperiment || "No experiment"}</p>
            <h2>{viewTitle(view)}</h2>
          </div>
          {view !== "home" && (
            <button className="topbar-cta" onClick={() => setView("create")}>
              <Wand2 size={15} /> New figure
            </button>
          )}
        </header>

        {error && <div className="error-banner">{error}</div>}

        {view === "home" && (
          <HomeView
            experiment={selectedExperiment}
            entries={entries}
            exports={exports}
            pipelines={pipelines}
            selectedPipelinePath={selectedPipelinePath}
            setSelectedPipelinePath={setSelectedPipelinePath}
            setView={setView}
          />
        )}
        {view === "create" && (
          <CreateFigureView
            experiment={selectedExperiment}
            entries={entries}
            sources={sources}
            pipelines={pipelines}
            selectedPipelinePath={selectedPipelinePath}
            setSelectedPipelinePath={setSelectedPipelinePath}
            refreshPipelines={async () => {
              const pipeData = await api.pipelines();
              setPipelines(pipeData);
            }}
            refreshExperimentData={async () => {
              if (!selectedExperiment) return;
              const [entryData, sourceData, processorData] = await Promise.all([
                api.entries(selectedExperiment),
                api.sources(selectedExperiment),
                api.processors(selectedExperiment)
              ]);
              setEntries(entryData);
              setSources(sourceData);
              setProcessors(processorData);
            }}
            setView={setView}
          />
        )}
        {view === "workbench" && (
          <Workbench
            groupedPipelines={groupedPipelines}
            selectedPipelinePath={selectedPipelinePath}
            setSelectedPipelinePath={setSelectedPipelinePath}
            pipeline={pipeline}
            selectedNode={selectedNode}
            setSelectedNode={setSelectedNode}
            experiment={selectedExperiment}
            runBusy={runBusy}
            runResult={runResult}
            onRun={runSelectedPipeline}
            yamlDraft={yamlDraft}
            yamlDirty={yamlDirty}
            saveBusy={saveBusy}
            onYamlChange={(value) => {
              setYamlDraft(value);
              setYamlDirty(value !== (pipeline?.yaml || ""));
            }}
            onSave={saveSelectedPipeline}
            onRename={renameSelectedPipeline}
            stageResults={stageResults}
            exports={exports}
            versions={versions}
            focusedVersionId={focusedVersionId}
            processors={processors}
            onSwitchVersion={switchToVersion}
            onOpenResults={(entry) => {
              setSelectedEntry(entry);
              setView("data");
            }}
          />
        )}
        {view === "data" && (
          <DataView
            experiment={selectedExperiment}
            entries={entries}
            exports={exports}
            versions={allVersions}
            selectedEntry={selectedEntry}
            setSelectedEntry={setSelectedEntry}
            preview={preview}
            lineage={lineage}
            setView={setView}
            onOpenPipelineVersion={openPipelineVersion}
            onSwitchPipelineVersion={switchAndOpenPipelineVersion}
          />
        )}
        {view === "processorStudio" && (
          <ProcessorStudio
            experiment={selectedExperiment}
            onSaved={async () => {
              if (!selectedExperiment) return;
              const processorData = await api.processors(selectedExperiment);
              setProcessors(processorData);
            }}
          />
        )}
        {view === "processors" && (
          <ProcessorsView
            experiment={selectedExperiment}
            selectedPipelinePath={selectedPipelinePath}
            pipelines={pipelines}
            setSelectedPipelinePath={setSelectedPipelinePath}
            onRefresh={async () => {
              if (!selectedExperiment) return;
              const [entryData, exportData, versionData, processorData] = await Promise.all([
                api.entries(selectedExperiment),
                api.exports(selectedExperiment),
                api.versions(selectedExperiment, selectedPipelinePath || undefined),
                api.processors(selectedExperiment)
              ]);
              setEntries(entryData);
              setExports(exportData);
              setVersions(versionData);
              setProcessors(processorData);
            }}
          />
        )}
      </main>
    </div>
  );
}

function viewTitle(view: View) {
  if (view === "home") return "FileLine Figure Studio";
  if (view === "create") return "Create Figure";
  if (view === "workbench") return "Pipelines";
  if (view === "data") return "Data";
  if (view === "processorStudio") return "Processor Studio";
  return "Storage and Settings";
}

function NavButton({ active, icon, label, onClick }: { active: boolean; icon: ReactNode; label: string; onClick: () => void }) {
  return (
    <button className={`nav-button ${active ? "active" : ""}`} onClick={onClick}>
      {icon}
      {label}
    </button>
  );
}

function HomeView({
  experiment,
  entries,
  exports,
  pipelines,
  selectedPipelinePath,
  setSelectedPipelinePath,
  setView
}: {
  experiment: string;
  entries: Entry[];
  exports: ExportItem[];
  pipelines: PipelineSummary[];
  selectedPipelinePath: string;
  setSelectedPipelinePath: (path: string) => void;
  setView: (view: View) => void;
}) {
  const recentResults = exports.slice(0, 3);
  const dataCount = entries.length;
  const primaryAction = exports.length
    ? { label: "View figures", icon: <ImageIcon size={17} />, view: "data" as View }
    : dataCount
      ? { label: "Run recommended pipeline", icon: <Play size={17} />, view: "workbench" as View }
      : { label: "Add data to start", icon: <Upload size={17} />, view: "create" as View };
  const statusText = exports.length
    ? `${exports.length} result${exports.length === 1 ? "" : "s"} ready to inspect.`
    : dataCount
      ? `${dataCount} data file${dataCount === 1 ? "" : "s"} available. Run a pipeline to create a figure.`
      : "This experiment has no data yet. Add a table or source pattern to start.";
  const suggested = currentExperimentPipelines(pipelines, experiment)
    .filter((item) => item.exportCount > 0 || item.processors.some((name) => name.includes("plot")))
    .slice(0, 3);

  return (
    <div className="home-grid">
      <section className="home-hero">
        <div>
          <div className="hero-kicker"><Sparkles size={16} /> Reproducible figures, without script archaeology</div>
          <h2>Turn experiment files into trusted figures.</h2>
          <p>
            {statusText}
          </p>
          <div className="hero-actions">
            <button className="hero-primary" onClick={() => setView(primaryAction.view)}>
              {primaryAction.icon} {primaryAction.label}
            </button>
          </div>
        </div>
      </section>

      <section className="panel home-panel">
        <div className="panel-header">
          <div>
            <h3>Recent outputs</h3>
            <p>{experiment || "No experiment selected"}</p>
          </div>
          {recentResults.length > 0 && <button className="compact-action" onClick={() => setView("data")}><Eye size={15} /> Open all in Data</button>}
        </div>
        <div className="result-card-grid">
          {recentResults.length > 0 ? recentResults.map((item) => (
            <button key={item.name} className="result-card" onClick={() => {
              setView("data");
            }}>
              <div className="result-card-icon"><FileBarChart size={18} /></div>
              <strong>{item.name}</strong>
              <span>ID {item.entryId}  -  {shortDate(item.createdAt)}</span>
              <em>Open in Data</em>
            </button>
          )) : (
            <div className="friendly-empty home-empty">
              <strong>No named outputs yet.</strong>
              <span>Run a pipeline with a final output export to populate this area.</span>
            </div>
          )}
        </div>
      </section>

      <section className="panel home-panel suggested-panel">
        <div className="panel-header">
          <div>
            <h3>Suggested pipelines</h3>
            <p>Optional shortcuts</p>
          </div>
        </div>
        <div className="suggested-list">
          {suggested.map((item) => (
            <button
              key={item.path}
              className={`suggested-row ${selectedPipelinePath === item.path ? "active" : ""}`}
              onClick={() => {
                setSelectedPipelinePath(item.path);
                setView("workbench");
              }}
            >
              <div>
                <strong>{item.name}</strong>
                <span>{item.group}  -  {item.stepCount} steps  -  {item.exportCount} outputs</span>
              </div>
              <ArrowRight size={16} />
            </button>
          ))}
        </div>
      </section>
    </div>
  );
}

function CreateFigureView({
  experiment,
  entries,
  sources,
  pipelines,
  selectedPipelinePath,
  setSelectedPipelinePath,
  refreshPipelines,
  refreshExperimentData,
  setView
}: {
  experiment: string;
  entries: Entry[];
  sources: SourceCandidate[];
  pipelines: PipelineSummary[];
  selectedPipelinePath: string;
  setSelectedPipelinePath: (path: string) => void;
  refreshPipelines: () => Promise<void>;
  refreshExperimentData: () => Promise<void>;
  setView: (view: View) => void;
}) {
  const [goal, setGoal] = useState("compare");
  const [dataQuery, setDataQuery] = useState("");
  const [pipelineQuery, setPipelineQuery] = useState("");
  const [sourceMode, setSourceMode] = useState<"registered" | "pattern">("registered");
  const [selectedSourcePath, setSelectedSourcePath] = useState("");
  const [sourcePattern, setSourcePattern] = useState("");
  const [remoteBase, setRemoteBase] = useState("");
  const [uploadBusy, setUploadBusy] = useState(false);
  const [processorBusy, setProcessorBusy] = useState(false);
  const [draftName, setDraftName] = useState(`studio_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}`);
  const [createBusy, setCreateBusy] = useState(false);
  const [createError, setCreateError] = useState("");
  const [chartPlan, setChartPlan] = useState<ChartPlan | null>(null);
  const [chartSetup, setChartSetup] = useState<ChartSetup>({});
  const [planBusy, setPlanBusy] = useState(false);
  const fallbackSources: SourceCandidate[] = entries
    .filter((entry) => entry.type === "raw")
    .map((entry) => ({
      entry,
      kind: entry.originalPath ? "cached" : "local",
      path: entry.originalPath || entry.path,
      directory: "",
      pattern: entry.originalPath || entry.path,
      label: entry.fileName || `Entry ${entry.id}`
    }));
  const availableSources = sources.length ? sources : fallbackSources;
  const filteredSources = availableSources
    .filter((source) =>
      `${source.label} ${source.path} ${source.entry.tags.join(" ")}`.toLowerCase().includes(dataQuery.toLowerCase())
    )
    .slice(0, 10);
  const selectedSource = availableSources.find((source) => source.path === selectedSourcePath) || filteredSources[0] || availableSources[0];
  const standardTemplates = standardTemplatePipelines(pipelines);
  const featured = standardTemplates
    .map((pipeline) => ({ pipeline, score: pipelineGoalScore(pipeline, goal) }))
    .sort((a, b) => b.score - a.score || a.pipeline.name.localeCompare(b.pipeline.name))
    .map((item) => item.pipeline);
  const pipelineSearch = pipelineQuery.trim().toLowerCase();
  const experimentPipelines = currentExperimentPipelines(pipelines, experiment)
    .filter((pipeline) => !featured.some((item) => item.path === pipeline.path))
    .filter((pipeline) => pipelineSearch && `${pipeline.name} ${pipeline.description} ${pipeline.path} ${pipeline.processors.join(" ")}`.toLowerCase().includes(pipelineSearch))
    .sort((a, b) => a.name.localeCompare(b.name));
  const recommended = [...featured, ...experimentPipelines];
  const selected = recommended.find((pipeline) => pipeline.path === selectedPipelinePath) || featured[0] || experimentPipelines[0];
  const chartColumns = chartPlan?.columns.map((column) => column.name) || [];
  const rawNumericChartColumns = chartPlan?.columns.filter((column) => column.kind === "numeric").map((column) => column.name) || [];
  const numericChartColumns = rawNumericChartColumns.length ? rawNumericChartColumns : chartColumns;
  const updateChartSetup = (patch: Partial<ChartSetup>) => setChartSetup((current) => ({ ...current, ...patch }));

  useEffect(() => {
    if (!selectedSourcePath && selectedSource?.path) {
      setSelectedSourcePath(selectedSource.path);
    }
  }, [selectedSource?.path, selectedSourcePath]);

  useEffect(() => {
    if (!selectedPipelinePath && selected?.path) {
      setSelectedPipelinePath(selected.path);
    }
  }, [selected?.path, selectedPipelinePath, setSelectedPipelinePath]);

  useEffect(() => {
    if (sourceMode !== "registered" || !experiment || !selectedSource?.entry?.id) {
      setChartPlan(null);
      setChartSetup({});
      return;
    }
    let cancelled = false;
    setPlanBusy(true);
    api.chartPlan(experiment, selectedSource.entry.id, goal)
      .then((plan) => {
        if (!cancelled) setChartPlan(plan);
      })
      .catch(() => {
        if (!cancelled) setChartPlan(null);
      })
      .finally(() => {
        if (!cancelled) setPlanBusy(false);
      });
    return () => {
      cancelled = true;
    };
  }, [experiment, selectedSource?.entry?.id, sourceMode, goal]);

  useEffect(() => {
    if (chartPlan?.supported) {
      setChartSetup(chartSetupFromPlan(chartPlan));
    }
  }, [chartPlan]);

  async function createDraftPipeline() {
    if (!selected) return;
    setCreateBusy(true);
    setCreateError("");
    try {
      const sourceSpec: SourceSpec | undefined = sourceMode === "pattern"
        ? {
            path: sourcePattern.trim() || "*",
            remote: remoteBase.trim() || undefined,
            source: "initial",
            tags: ["studio_input"],
            type: "raw"
          }
        : selectedSource
          ? {
              path: selectedSource.entry.path || selectedSource.pattern || selectedSource.path,
              source: "initial",
              tags: ["studio_input"],
              type: "raw"
            }
          : undefined;
      const detail = await api.clonePipeline(selected.path, draftName, experiment, selectedSource?.entry.id, sourceSpec);
      await refreshPipelines();
      setSelectedPipelinePath(detail.path);
      setView("workbench");
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : String(err));
    } finally {
      setCreateBusy(false);
    }
  }

  async function createAutoFigure() {
    if (!experiment || !selectedSource?.entry?.id || !chartPlan?.supported) return;
    setCreateBusy(true);
    setCreateError("");
    try {
      const detail = await api.createAutoChart(experiment, selectedSource.entry.id, draftName, goal, chartSetup);
      await refreshPipelines();
      setSelectedPipelinePath(detail.path);
      setView("workbench");
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : String(err));
    } finally {
      setCreateBusy(false);
    }
  }

  async function uploadSourceFile(file: File | null) {
    if (!file || !experiment) return;
    setUploadBusy(true);
    setCreateError("");
    try {
      const source = await api.uploadSource(experiment, file);
      await refreshExperimentData();
      setSourceMode("registered");
      setSelectedSourcePath(source.path);
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : String(err));
    } finally {
      setUploadBusy(false);
    }
  }

  async function uploadProcessorFile(file: File | null) {
    if (!file || !experiment) return;
    setProcessorBusy(true);
    setCreateError("");
    try {
      await api.uploadProcessor(experiment, file);
      await refreshExperimentData();
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : String(err));
    } finally {
      setProcessorBusy(false);
    }
  }

  return (
    <div className="create-grid">
      <section className="create-header">
        <div>
          <div className="hero-kicker"><Wand2 size={16} /> Guided workflow</div>
          <h2>Create a figure without starting from YAML.</h2>
          <p>Upload a common table and FileLine will suggest a chart automatically. Keep the manual pipeline route for special cases.</p>
        </div>
      </section>

      <section className="panel create-step">
        <div className="panel-header">
          <div>
            <h3>1. Choose source</h3>
            <p>Use registered original data, or point FileLine at a new source pattern</p>
          </div>
        </div>
        <div className="source-mode-toggle">
          <button className={sourceMode === "registered" ? "active" : ""} onClick={() => setSourceMode("registered")}>
            Registered sources
          </button>
          <button className={sourceMode === "pattern" ? "active" : ""} onClick={() => setSourceMode("pattern")}>
            New pattern
          </button>
        </div>
        <div className="upload-strip">
          <label>
            <Upload size={15} />
            <span>{uploadBusy ? "Uploading data..." : "Upload data"}</span>
            <input type="file" onChange={(event) => uploadSourceFile(event.target.files?.[0] || null)} />
          </label>
        </div>
        <div className="search-box">
          <Search size={15} />
          <input value={dataQuery} onChange={(event) => setDataQuery(event.target.value)} placeholder="Search source path, file, tag" />
        </div>
        <div className="create-data-list">
          {sourceMode === "registered" ? (
            filteredSources.length ? filteredSources.map((source) => (
              <button key={`${source.path}-${source.entry.id}`} className={`create-data-row ${selectedSourcePath === source.path ? "active" : ""}`} onClick={() => setSelectedSourcePath(source.path)}>
                <Database size={16} />
                <div>
                  <strong>{source.label || source.entry.fileName || `Source ${source.entry.id}`}</strong>
                  <span>{source.kind}  -  {source.path || source.entry.originalPath || source.entry.path}</span>
                </div>
              </button>
            )) : (
            <div className="friendly-empty">
              <strong>No registered sources found.</strong>
              <span>Switch to New pattern to load from a local glob or remote source.</span>
              <div className="empty-actions">
                <button className="compact-action" onClick={() => setSourceMode("pattern")}>
                  <Database size={15} /> Add pattern
                </button>
              </div>
            </div>
            )
          ) : (
            <div className="source-pattern-card">
              <label className="draft-name-field">
                <span>Source path or glob</span>
                <input value={sourcePattern} onChange={(event) => setSourcePattern(event.target.value)} placeholder="/data/runs/**/*.csv or logs/*.log" />
              </label>
              <label className="draft-name-field">
                <span>Remote base optional</span>
                <input value={remoteBase} onChange={(event) => setRemoteBase(event.target.value)} placeholder="user@host:22:/remote/root" />
              </label>
              <small>FileLine will write this into initial_load.include, so future runs keep the real source pattern.</small>
            </div>
          )}
          <details className="advanced-block create-source-advanced">
            <summary><FileCode size={15} /> Advanced source setup</summary>
            <label className="processor-upload-inline">
              <FileCode size={15} />
              <span>{processorBusy ? "Registering..." : "Upload processor script"}</span>
              <input type="file" accept=".py" onChange={(event) => uploadProcessorFile(event.target.files?.[0] || null)} />
            </label>
          </details>
        </div>
      </section>

      <section className="panel create-step">
        <div className="panel-header">
          <div>
            <h3>2. What should FileLine make?</h3>
            <p>Common table-like data can go straight to a chart; custom logs can use an uploaded processor first.</p>
          </div>
        </div>
        <div className="goal-grid">
          <GoalButton active={goal === "compare"} icon={<ChartNoAxesCombined size={18} />} title="Bar comparison" text="Compare methods, systems, models, or experimental settings." onClick={() => setGoal("compare")} />
          <GoalButton active={goal === "trend"} icon={<Activity size={18} />} title="Line or curve" text="Plot loss, throughput, accuracy, or any metric over time." onClick={() => setGoal("trend")} />
          <GoalButton active={goal === "timeline"} icon={<GitBranch size={18} />} title="Timeline" text="Show stages, events, GPU activity, or execution overlap." onClick={() => setGoal("timeline")} />
          <GoalButton active={goal === "breakdown"} icon={<Layers size={18} />} title="Breakdown" text="Stack, group, filter, or aggregate data before plotting." onClick={() => setGoal("breakdown")} />
        </div>
        <div className="auto-plan-panel">
          {planBusy ? (
            <div className="friendly-empty">
              <strong>Analyzing source...</strong>
              <span>FileLine is inferring columns and a chart type from the selected data.</span>
            </div>
          ) : chartPlan?.supported ? (
            <>
              <div className="auto-plan-header">
                <div>
                  <strong>{chartPlan.chartType.replace("_", " ")}</strong>
                  <span>{chartPlan.reason}</span>
                </div>
                <small>{chartPlan.confidence} confidence</small>
              </div>
              <div className="chart-setup-grid">
                <label className="chart-setup-field title-field">
                  <span>Chart title</span>
                  <input value={chartSetup.title || ""} onChange={(event) => updateChartSetup({ title: event.target.value })} placeholder="Figure title" />
                </label>
                <label className="chart-setup-field">
                  <span>Chart type</span>
                  <select value={chartSetup.chartType || chartPlan.chartType || "line"} onChange={(event) => updateChartSetup({ chartType: event.target.value })}>
                    <option value="line">Line</option>
                    <option value="bar">Bar</option>
                    <option value="grouped_bar">Grouped bar</option>
                    <option value="horizontal_bar">Horizontal bar</option>
                  </select>
                </label>
                <label className="chart-setup-field">
                  <span>X axis</span>
                  <select value={chartSetup.xCol || ""} onChange={(event) => updateChartSetup({ xCol: event.target.value })}>
                    <option value="">Auto</option>
                    {chartColumns.map((column) => <option key={column} value={column}>{column}</option>)}
                  </select>
                </label>
                <label className="chart-setup-field">
                  <span>Y / value</span>
                  <select value={chartSetup.yCol || ""} onChange={(event) => updateChartSetup({ yCol: event.target.value })}>
                    <option value="">Auto</option>
                    {numericChartColumns.map((column) => <option key={column} value={column}>{column}</option>)}
                  </select>
                </label>
                <label className="chart-setup-field">
                  <span>Group by</span>
                  <select value={chartSetup.groupCol || ""} onChange={(event) => updateChartSetup({ groupCol: event.target.value })}>
                    <option value="">None</option>
                    {chartColumns.map((column) => <option key={column} value={column}>{column}</option>)}
                  </select>
                </label>
              </div>
              <div className="auto-plan-columns compact">
                {chartPlan.columns.slice(0, 4).map((column) => (
                  <div key={column.name} className="auto-column-chip">
                    <strong>{column.name}</strong>
                    <span>{column.kind}  -  {column.uniqueCount} unique</span>
                  </div>
                ))}
              </div>
              <details className="advanced-block create-preview-details">
                <summary><Table2 size={15} /> Data preview and inferred params</summary>
                <div className="auto-plan-fields">
                  {Object.entries(chartPlan.params).filter(([, value]) => value !== null && value !== "").map(([key, value]) => (
                    <div key={key}>
                      <span>{key}</span>
                      <strong>{String(value)}</strong>
                    </div>
                  ))}
                </div>
                {chartPlan.sampleRows.length > 0 && (
                  <div className="compact-table-preview">
                    <p>{chartPlan.shape[0]} rows x {chartPlan.shape[1]} columns</p>
                    <table>
                      <thead>
                        <tr>{Object.keys(chartPlan.sampleRows[0]).slice(0, 5).map((column) => <th key={column}>{column}</th>)}</tr>
                      </thead>
                      <tbody>
                        {chartPlan.sampleRows.slice(0, 4).map((row, index) => (
                          <tr key={index}>
                            {Object.keys(chartPlan.sampleRows[0]).slice(0, 5).map((column) => <td key={column}>{String(row[column] ?? "")}</td>)}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </details>
            </>
          ) : (
            <div className="friendly-empty">
              <strong>No reliable auto-chart yet.</strong>
              <span>
                Upload a table-like file, or add a custom processor script first if the source needs parsing before plotting.
              </span>
            </div>
          )}
        </div>
      </section>

      <section className="panel create-step create-confirm-panel">
        <div className="panel-header">
          <div>
            <h3>3. Confirm and create</h3>
            <p>Name the result, then FileLine will open the editable pipeline.</p>
          </div>
        </div>
        <div className="create-confirm-body">
          <label className="draft-name-field">
            <span>Result / pipeline name</span>
            <input value={draftName} onChange={(event) => setDraftName(event.target.value)} placeholder="my_throughput_figure" />
          </label>
          <div className="create-confirm-summary">
            <div>
              <span>Source</span>
              <strong>{sourceMode === "pattern" ? sourcePattern || "New source pattern" : selectedSource?.label || selectedSource?.entry.fileName || "Selected source"}</strong>
            </div>
            <div>
              <span>Figure path</span>
              <strong>{chartPlan?.supported ? (chartSetup.chartType || chartPlan.chartType).replace("_", " ") : selected?.name || "Manual pipeline"}</strong>
            </div>
          </div>
          {createError && <div className="error-banner compact">{createError}</div>}
          <button
            className="create-draft-button"
            disabled={planBusy || (chartPlan?.supported ? !selectedSource?.entry?.id : !selected) || createBusy || !draftName.trim()}
            onClick={chartPlan?.supported ? createAutoFigure : createDraftPipeline}
          >
            <Wand2 size={16} /> {createBusy ? "Creating..." : planBusy ? "Analyzing source..." : chartPlan?.supported ? "Create auto figure" : "Create new pipeline"}
          </button>
        </div>
      </section>

      <details className="panel create-step create-template-panel create-advanced">
        <summary className="create-advanced-summary">
          <div>
            <h3>Advanced pipeline options</h3>
            <p>Choose a standard start, or search existing YAML to copy into a new pipeline.</p>
          </div>
        </summary>
        <div className="template-list">
          {featured.length > 0 && <div className="template-section-title">Standard templates</div>}
          {featured.map((pipeline) => (
            <button
              key={pipeline.path}
              className={`template-row ${selectedPipelinePath === pipeline.path ? "active" : ""}`}
              onClick={() => setSelectedPipelinePath(pipeline.path)}
            >
              <div>
                <strong>{pipeline.name}</strong>
                <span>{pipeline.description || "Reusable starting point for a new figure pipeline."}</span>
                <span>{pipeline.group}  -  {pipeline.stepCount} steps  -  {pipeline.processors.slice(0, 3).join(", ")}</span>
              </div>
              <span className="template-action-label">{selectedPipelinePath === pipeline.path ? "Selected" : "Use as start"}</span>
            </button>
          ))}
          <div className="template-section-title">Copy existing pipeline</div>
          <div className="template-search">
            <Search size={15} />
            <input value={pipelineQuery} onChange={(event) => setPipelineQuery(event.target.value)} placeholder="Search existing YAML by name, processor, or path" />
          </div>
          {experimentPipelines.map((pipeline) => (
            <button
              key={pipeline.path}
              className={`template-row ${selectedPipelinePath === pipeline.path ? "active" : ""}`}
              onClick={() => setSelectedPipelinePath(pipeline.path)}
            >
              <div>
                <strong>{pipeline.name}</strong>
                <span>{pipeline.description || pipeline.path}</span>
                <span>{pipeline.group}  -  {pipeline.stepCount} steps  -  {pipeline.exportCount} outputs</span>
              </div>
              <span className="template-action-label">{selectedPipelinePath === pipeline.path ? "Selected" : "Use as start"}</span>
            </button>
          ))}
          {pipelineQuery.trim() && experimentPipelines.length === 0 && (
            <div className="friendly-empty compact-empty">
              <strong>No matching pipeline.</strong>
              <span>Try a different name, processor, or path.</span>
            </div>
          )}
          {chartPlan?.supported && selectedSource?.entry?.id && (
            <button className="secondary-auto-button" disabled={createBusy} onClick={createAutoFigure}>
              <Wand2 size={16} /> {createBusy ? "Creating..." : "Use detected chart"}
            </button>
          )}
        </div>
      </details>
    </div>
  );
}

function GoalButton({ active, icon, title, text, onClick }: { active: boolean; icon: ReactNode; title: string; text: string; onClick: () => void }) {
  return (
    <button className={`goal-button ${active ? "active" : ""}`} onClick={onClick}>
      <span>{icon}</span>
      <strong>{title}</strong>
      <small>{text}</small>
    </button>
  );
}

function chartSetupFromPlan(plan: ChartPlan | null): ChartSetup {
  if (!plan) return {};
  const params = plan.params || {};
  const pick = (keys: string[]) => {
    for (const key of keys) {
      const value = params[key];
      if (value !== null && value !== undefined && value !== "") return String(value);
    }
    return "";
  };
  return {
    chartType: plan.chartType || "auto",
    title: String(params.title || plan.title || ""),
    xCol: pick(["x_col", "time_col", "main_group_col", "y_col", "category_col", "start_col"]),
    yCol: pick(["value_col", "end_col"]),
    groupCol: pick(["tag_col", "sub_group_col", "sub_category_col"]),
  };
}

function Workbench({
  groupedPipelines,
  selectedPipelinePath,
  setSelectedPipelinePath,
  pipeline,
  selectedNode,
  setSelectedNode,
  experiment,
  runBusy,
  runResult,
  onRun,
  yamlDraft,
  yamlDirty,
  saveBusy,
  onYamlChange,
  onSave,
  onRename,
  stageResults,
  exports,
  processors,
  versions,
  focusedVersionId,
  onSwitchVersion,
  onOpenResults
}: {
  groupedPipelines: [string, PipelineSummary[]][];
  processors: Processor[];
  selectedPipelinePath: string;
  setSelectedPipelinePath: (value: string) => void;
  pipeline: PipelineDetail | null;
  selectedNode: GraphNode | null;
  setSelectedNode: (node: GraphNode | null) => void;
  experiment: string;
  runBusy: boolean;
  runResult: PipelineRunResult | null;
  onRun: (dryRun: boolean, forceFresh?: boolean, sourceMode?: string) => void;
  yamlDraft: string;
  yamlDirty: boolean;
  saveBusy: boolean;
  onYamlChange: (value: string) => void;
  onSave: () => void;
  onRename: (name: string) => Promise<void>;
  stageResults: StageResults;
  exports: ExportItem[];
  onOpenResults: (entry: Entry) => void;
  versions: Version[];
  focusedVersionId: number | null;
  onSwitchVersion: (version: Version) => void;
}) {
  const [workbenchMode, setWorkbenchMode] = useState<"story" | "graph">("story");
  const [runSourceMode, setRunSourceMode] = useState<"version" | "external">("version");
  const [forceRecompute, setForceRecompute] = useState(false);
  const [renameOpen, setRenameOpen] = useState(false);
  const [renameName, setRenameName] = useState("");
  const [renameBusy, setRenameBusy] = useState(false);
  const [renameError, setRenameError] = useState("");
  const flatPipelines = groupedPipelines.flatMap(([, items]) => items);

  // Note: comment text was normalized to avoid mojibake.
  const liveGraph = useMemo(() => {
    if (!yamlDraft) return pipeline?.graph || { nodes: [], edges: [] };
    try {
      const cfg = yaml.load(yamlDraft) as Record<string, unknown> | null;
      if (!cfg || typeof cfg !== "object") return pipeline?.graph || { nodes: [], edges: [] };

      const nodes: GraphNode[] = [];
      const edges: Array<{ id: string; source: string; target: string; label: string }> = [];
      const varToNode = new Map<string, string>();

      const includes = (((cfg as Record<string, unknown>).initial_load as Record<string, unknown>)?.include as Array<Record<string, unknown>>) || [];
      const sourceGroups = new Map<string, typeof includes>();
      for (const inc of includes) {
        const src = String(inc.source || "initial");
        sourceGroups.set(src, [...(sourceGroups.get(src) || []), inc]);
      }
      if (sourceGroups.size === 0) sourceGroups.set("initial", []);
      for (const [srcName, srcIncludes] of sourceGroups) {
        const sid = `source:${srcName}`;
        nodes.push({ id: sid, type: "source", label: srcName, subtitle: `${srcIncludes.length} include pattern(s)`, meta: { includes: srcIncludes } });
        varToNode.set(srcName, sid);
      }

      const steps = (cfg as Record<string, unknown>).steps as Array<Record<string, unknown>> || [];
      for (let i = 0; i < steps.length; i++) {
        const step = steps[i];
        const nodeId = `step:${i + 1}`;
        const proc = String(step.processor || "unknown");
        const outVar = String(step.output || `step_${i + 1}`);
        const outputs = step.outputs as Record<string, string> | undefined;
        nodes.push({
          id: nodeId, type: "processor", label: proc,
          subtitle: outputs ? `${Object.keys(outputs).length} branches` : outVar,
          meta: { index: i + 1, inputs: step.inputs || "initial", output: outVar, outputs: outputs || null, params: step.params || {}, export: step.export || null }
        });
        const rawInputs = step.inputs;
        const inputVars = Array.isArray(rawInputs) ? rawInputs : [rawInputs || "initial"];
        const fallbackSrc = varToNode.values().next().value || "source:initial";
        for (const iv of inputVars) {
          const from = varToNode.get(String(iv)) || fallbackSrc;
          edges.push({ id: `${from}->${nodeId}:${iv}`, source: from, target: nodeId, label: String(iv) });
        }
        if (outputs) {
          for (const [, varName] of Object.entries(outputs)) varToNode.set(varName, nodeId);
        } else {
          varToNode.set(outVar, nodeId);
        }
      }

      const finals = (cfg as Record<string, unknown>).final_output as Array<Record<string, unknown>> || [];
      for (let i = 0; i < finals.length; i++) {
        const f = finals[i];
        const fid = `export:${i + 1}`;
        const fName = String(f.name || "");
        const fExport = String(f.export || fName);
        const srcVar = varToNode.get(fName);
        nodes.push({ id: fid, type: "export", label: fExport, subtitle: fName, meta: { ...f, source: fName, upstreamIndex: srcVar ? parseInt(srcVar.split(":")[1]) : null } });
        if (srcVar) edges.push({ id: `${srcVar}->${fid}:${fName}`, source: srcVar, target: fid, label: fName });
      }
      return { nodes, edges };
    } catch { return pipeline?.graph || { nodes: [], edges: [] }; }
  }, [yamlDraft, pipeline]);

  const finalEntry = latestFinalEntry(pipeline, stageResults, exports);
  const finalSourceSummary = sourceSummaryFromNodes(liveGraph.nodes);
  const sourceCount = liveGraph.nodes.filter((node) => node.type === "source").length;
  const stepCount = liveGraph.nodes.filter((node) => node.type === "processor").length;
  const outputCount = liveGraph.nodes.filter((node) => node.type === "export").length;
  const savedResultCount = Object.values(stageResults).reduce((total, entriesForStage) => total + entriesForStage.length, 0);
  const trimmedRenameName = renameName.trim();

  function openRenameDialog() {
    if (!pipeline) return;
    setRenameName(pipeline.name || "");
    setRenameError("");
    setRenameOpen(true);
  }

  async function submitRename() {
    if (!pipeline) return;
    if (!trimmedRenameName) {
      setRenameError("Pipeline name cannot be empty.");
      return;
    }
    if (yamlDirty) {
      setRenameError("Save or reset current pipeline changes before renaming.");
      return;
    }
    setRenameBusy(true);
    setRenameError("");
    try {
      await onRename(trimmedRenameName);
      setRenameOpen(false);
    } catch (err) {
      setRenameError(err instanceof Error ? err.message : String(err));
    } finally {
      setRenameBusy(false);
    }
  }

  const flow = useMemo(() => {
    if (!pipeline && !yamlDraft) return { nodes: [], edges: [] };
    const graph = liveGraph;
    if (!graph.nodes.length) return { nodes: [], edges: [] };
    const depths = new Map<string, number>();
    const incoming = new Map<string, string[]>();
    graph.nodes.forEach((node) => incoming.set(node.id, []));
    graph.edges.forEach((edge) => incoming.set(edge.target, [...(incoming.get(edge.target) || []), edge.source]));
    function depth(id: string): number {
      if (depths.has(id)) return depths.get(id)!;
      const parents = incoming.get(id) || [];
      const value = parents.length ? Math.max(...parents.map(depth)) + 1 : 0;
      depths.set(id, value);
      return value;
    }
    graph.nodes.forEach((node) => depth(node.id));
    const rows = new Map<number, number>();
    const nodes: Node[] = graph.nodes.map((node) => {
      const d = depths.get(node.id) || 0;
      const row = rows.get(d) || 0;
      rows.set(d, row + 1);
      return {
        id: node.id,
        type: "fileline",
        position: { x: d * 270, y: row * 130 },
        data: node
      };
    });
    const edges: Edge[] = graph.edges.map((edge) => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      label: edge.label,
      animated: edge.target.startsWith("export"),
      style: { stroke: "#8191a8", strokeWidth: 1.6 }
    }));
    return { nodes, edges };
  }, [liveGraph]);

  return (
    <div className="workbench-grid">
      <section className="panel graph-panel">
        <div className="panel-header graph-header">
          <div>
            <div className="pipeline-title-row">
              <h3>{pipeline?.name || "No pipeline selected"}</h3>
              {pipeline && (
                <button className="title-action-button" onClick={openRenameDialog} disabled={renameBusy || saveBusy} title="Rename pipeline">
                  <FileCode size={14} /> Rename
                </button>
              )}
            </div>
            <p>{workbenchMode === "story" ? "Run story" : "Advanced DAG"}</p>
          </div>
          <div className="run-actions">
            <label className="pipeline-picker">
              <span>Template</span>
              <select value={selectedPipelinePath} onChange={(event) => setSelectedPipelinePath(event.target.value)}>
                {flatPipelines.map((item) => (
                  <option key={item.path} value={item.path}>
                    {item.name}  -  {item.group}
                  </option>
                ))}
              </select>
            </label>
            <div className="mode-toggle">
              <button className={workbenchMode === "story" ? "active" : ""} onClick={() => setWorkbenchMode("story")}>Story</button>
              <button className={workbenchMode === "graph" ? "active" : ""} onClick={() => setWorkbenchMode("graph")}>Graph</button>
            </div>
            <details className="run-options">
              <summary><SlidersHorizontal size={15} /> Run settings</summary>
              <div className="run-options-popover">
                <label>
                  <span>Data</span>
                  <select value={runSourceMode} onChange={(event) => setRunSourceMode(event.target.value as "version" | "external")}>
                    <option value="version">Version data</option>
                    <option value="external">Latest source</option>
                  </select>
                  <small className="run-helper">
                    Version data reuses the data saved with this pipeline version. Latest source reloads the YAML source pattern.
                  </small>
                </label>
                <label className="run-checkbox">
                  <input type="checkbox" checked={forceRecompute} onChange={(event) => setForceRecompute(event.target.checked)} />
                  <span>
                    Recompute processors
                    <small className="run-helper">Run the same data through every step again, bypassing cached step outputs.</small>
                  </span>
                </label>
                <div className="dry-run-option">
                  <button className="secondary-run-button" disabled={!pipeline || runBusy} onClick={() => onRun(true, false, runSourceMode)}>
                    <Activity size={15} /> Check flow
                  </button>
                  <small className="run-helper">Preview matched files and planned steps without creating data, plots, or versions.</small>
                </div>
              </div>
            </details>
            <button className="run-button" disabled={!pipeline || !experiment || runBusy} onClick={() => onRun(false, forceRecompute, runSourceMode)}>
              <Play size={15} /> {runBusy ? "Generating" : "Generate result"}
            </button>
          </div>
        </div>
        {renameOpen && pipeline && (
          <RenamePipelineDialog
            value={renameName}
            busy={renameBusy}
            error={renameError}
            currentPath={pipeline.path}
            onChange={(value) => {
              setRenameName(value);
              setRenameError("");
            }}
            onCancel={() => {
              setRenameOpen(false);
              setRenameError("");
            }}
            onSubmit={submitRename}
          />
        )}
        <div className="run-note">
          <span>{sourceCount || 0} input / {stepCount || 0} steps / {outputCount || 0} output / {savedResultCount} saved results</span>
        <p>Check flow previews inputs and steps. Generate result writes the final output and records a version.</p>
        </div>
        {workbenchMode === "story" ? (
          <StoryView
            experiment={experiment}
            pipeline={pipeline}
            liveGraph={liveGraph}
            stageResults={stageResults}
            selectedNode={selectedNode}
            onSelectNode={setSelectedNode}
            finalEntry={finalEntry}
            sourceSummary={finalSourceSummary}
            onOpenResults={onOpenResults}
          />
        ) : (
          <>
            <div className="graph-canvas">
              <ReactFlow
                nodes={flow.nodes}
                edges={flow.edges}
                nodeTypes={nodeTypes}
                fitView
                onNodeClick={(_, node) => setSelectedNode(node.data as GraphNode)}
              >
                <MiniMap pannable zoomable />
                <Controls />
                <Background gap={18} size={1} />
              </ReactFlow>
            </div>
            <details className="graph-preview" open={finalEntry !== null}>
              <summary>Final Output</summary>
              <FinalOutputSpotlight experiment={experiment} entry={finalEntry} exportLabel={liveGraph.nodes.find(n => n.type === "export")?.label || ""} sourceSummary={finalSourceSummary} onOpenResults={onOpenResults} />
            </details>
          </>
        )}
      </section>

      <StageResultPanel
        experiment={experiment}
        pipeline={pipeline}
        selectedNode={selectedNode}
        runResult={runResult}
        yamlDraft={yamlDraft}
        yamlDirty={yamlDirty}
        saveBusy={saveBusy}
        onYamlChange={onYamlChange}
        onSave={onSave}
        stageResults={stageResults}
        processors={processors}
        versions={versions}
        focusedVersionId={focusedVersionId}
        onSwitchVersion={onSwitchVersion}
      />
    </div>
  );
}

function RenamePipelineDialog({
  value,
  busy,
  error,
  currentPath,
  onChange,
  onCancel,
  onSubmit
}: {
  value: string;
  busy: boolean;
  error: string;
  currentPath: string;
  onChange: (value: string) => void;
  onCancel: () => void;
  onSubmit: () => void;
}) {
  return (
    <div className="preview-modal-backdrop compact-modal-backdrop" role="dialog" aria-modal="true" aria-label="Rename pipeline">
      <section className="rename-dialog">
        <div className="rename-dialog-header">
          <div>
            <h3>Rename pipeline</h3>
            <p>{currentPath}</p>
          </div>
          <button className="icon-action" onClick={onCancel} title="Close rename dialog">
            <X size={16} />
          </button>
        </div>
        <label className="rename-field">
          <span>Name</span>
          <input
            value={value}
            onChange={(event) => onChange(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") onSubmit();
              if (event.key === "Escape") onCancel();
            }}
            autoFocus
          />
        </label>
        <p className="rename-hint">The YAML display name and pipeline file name will be updated together.</p>
        {error && <div className="inline-error">{error}</div>}
        <div className="rename-actions">
          <button className="secondary-save-button" disabled={busy} onClick={onCancel}>Cancel</button>
          <button className="primary-save-button" disabled={busy || !value.trim()} onClick={onSubmit}>
            {busy ? "Renaming" : "Rename"}
          </button>
        </div>
      </section>
    </div>
  );
}

function StageResultPanel({
  experiment,
  pipeline,
  selectedNode,
  runResult,
  yamlDraft,
  yamlDirty,
  saveBusy,
  onYamlChange,
  onSave,
  stageResults,
  processors,
  versions,
  focusedVersionId,
  onSwitchVersion
}: {
  experiment: string;
  pipeline: PipelineDetail | null;
  selectedNode: GraphNode | null;
  runResult: PipelineRunResult | null;
  yamlDraft: string;
  yamlDirty: boolean;
  saveBusy: boolean;
  onYamlChange: (value: string) => void;
  onSave: () => void;
  stageResults: StageResults;
  processors: Processor[];
  versions: Version[];
  focusedVersionId: number | null;
  onSwitchVersion: (version: Version) => void;
}) {
  const [activeResultId, setActiveResultId] = useState<number | null>(null);
  const [stagePreview, setStagePreview] = useState<Preview | null>(null);
  const [previewExpanded, setPreviewExpanded] = useState(false);
  const results = selectedNode ? stageResults[selectedNode.id] || [] : [];
  const activeEntry = results.find((entry) => entry.id === activeResultId) || results[0] || null;

  useEffect(() => {
    setActiveResultId(null);
    setPreviewExpanded(false);
  }, [selectedNode?.id]);

  useEffect(() => {
    if (!activeEntry) {
      setStagePreview(null);
      setPreviewExpanded(false);
      return;
    }
    if (!experiment) return;
    api.preview(experiment, activeEntry.id)
      .then(setStagePreview)
      .catch(() => setStagePreview(null));
  }, [experiment, activeEntry?.id]);

  return (
    <section className="panel inspector">
      <div className="panel-header">
        <div>
          <h3>Stage results</h3>
          <p>{selectedNode ? friendlyStageType(selectedNode) : "Select a stage"}</p>
        </div>
      </div>
      {selectedNode ? (
        <div className="inspector-body">
          <div className="inspector-title">
            <Eye size={18} />
            <strong>{selectedNode.label}</strong>
          </div>
          <StageSummary node={selectedNode} resultCount={results.length} />

          <ParamsEditor
            node={selectedNode}
            yamlDraft={yamlDraft}
            onYamlChange={onYamlChange}
            processors={processors}
          />
          {yamlDirty && (
            <div className="visible-save-bar">
              <div>
                <strong>Unsaved pipeline changes</strong>
                <span>Save parameter edits without opening Advanced YAML.</span>
              </div>
              <div className="visible-save-actions">
                <button className="primary-save-button" disabled={saveBusy} onClick={onSave}>
                  {saveBusy ? "Saving" : "Save changes"}
                </button>
                <button className="secondary-save-button" disabled={saveBusy} onClick={() => onYamlChange(pipeline?.yaml || "")}>
                  Reset
                </button>
              </div>
            </div>
          )}

          {results.length > 0 ? (
            <div className="stage-results">
              <div className="stage-result-tabs">
                {results.slice(0, 8).map((entry) => (
                  <button
                    key={entry.id}
                    className={activeEntry?.id === entry.id ? "active" : ""}
                    onClick={() => setActiveResultId(entry.id)}
                  >
                    {selectedNode.type === "export" ? selectedNode.label : entry.extension || entry.type}
                  </button>
                ))}
              </div>
              {activeEntry && (
                <div className="stage-card">
                  <div>
                    <strong>{selectedNode.type === "export" ? selectedNode.label : activeEntry.fileName || `Entry ${activeEntry.id}`}</strong>
                    <span>{shortDate(activeEntry.timestamp)}</span>
                  </div>
                  <small>{activeEntry.description || activeEntry.path}</small>
                </div>
              )}
              {activeEntry && stagePreview && (
                <button className="preview-expand-strip" onClick={() => setPreviewExpanded(true)}>
                  <Maximize2 size={15} /> Open large preview
                </button>
              )}
              {stagePreview?.kind === "table" && <CompactTablePreview preview={stagePreview} />}
              {stagePreview?.kind === "text" && <pre className="text-preview compact">{stagePreview.text.slice(0, 3000)}</pre>}
              {stagePreview?.kind === "file" && activeEntry?.extension === ".pdf" && (
                <PdfViewer url={api.fileUrl(experiment, activeEntry.id)} title={selectedNode.type === "export" ? selectedNode.label : activeEntry.fileName} />
              )}
              {stagePreview?.kind === "file" && activeEntry && [".png", ".jpg", ".jpeg", ".svg"].includes(activeEntry.extension) && (
                <img className="stage-image" src={api.fileUrl(experiment, activeEntry.id)} alt={selectedNode.type === "export" ? selectedNode.label : activeEntry.fileName} />
              )}
              {previewExpanded && activeEntry && stagePreview && (
                <StagePreviewModal
                  experiment={experiment}
                  entry={activeEntry}
                  title={selectedNode.type === "export" ? selectedNode.label : activeEntry.fileName || `Entry ${activeEntry.id}`}
                  preview={stagePreview}
                  onClose={() => setPreviewExpanded(false)}
                />
              )}
            </div>
          ) : (
            <div className="friendly-empty">
              <strong>No saved result found for this stage yet.</strong>
              <span>Run the pipeline, or choose a stage that has already produced data in the current experiment.</span>
            </div>
          )}

          {pipeline && (
            <div className="step-actions">
              <button className="compact-action" onClick={() => {
                if (!yamlDraft || typeof yamlDraft !== "string") return;
                try {
                  const cfg = yaml.load(yamlDraft);
                  const selIdx = selectedNode?.type === "processor" ? ((selectedNode.meta as Record<string, unknown>)?.index as number) || 0 : 0;
                  if (!cfg || typeof cfg !== "object") {
                    const fp = processors.some((processor) => processor.name === "file_identity") ? "file_identity" : "plot_line";
                    onYamlChange(yamlDraft + `\nsteps:\n  - processor: ${fp}\n    inputs: initial\n    output: step1\n    params: {}\n`);
                    return;
                  }
                  const config = cfg as YamlConfig;
                  const steps: YamlStep[] = Array.isArray(config.steps) ? config.steps as YamlStep[] : [];
                  config.steps = steps;
                  const finals: YamlStep[] = Array.isArray(config.final_output) ? config.final_output as YamlStep[] : [];
                  const defaultProc = processors.some((processor) => processor.name === "file_identity") ? "file_identity" : "plot_line";
                  let insertAt = steps.length;
                  let inVar = steps.length ? stepOutputVars(steps[steps.length - 1], steps.length)[0] : "initial";
                  if (selectedNode?.type === "source") {
                    insertAt = 0;
                    inVar = selectedNode.label || "initial";
                  } else if (selIdx > 0 && selIdx <= steps.length) {
                    insertAt = selIdx;
                    inVar = stepOutputVars(steps[selIdx - 1], selIdx)[0] || `step_${selIdx}`;
                  }
                  const newOut = uniqueOutputName(steps, defaultProc === "file_identity" ? "identity" : "step");
                  steps.splice(insertAt, 0, {
                    processor: defaultProc,
                    inputs: inVar,
                    output: newOut,
                    params: defaultParamsForProcessor(processors, defaultProc)
                  });
                  const target = new Set([inVar]);
                  for (let i = insertAt + 1; i < steps.length; i++) {
                    steps[i].inputs = replaceInputRefs(steps[i].inputs, target, newOut);
                  }
                  replaceFinalRefs(finals, target, newOut);
                  const out = yaml.dump(cfg, { indent: 2, noRefs: true, sortKeys: false });
                  if (out) onYamlChange(out); else console.warn("yaml.dump returned empty");
                } catch (e) { console.error("Add step failed:", e); }
              }}>+ Add step</button>
              <button className="compact-action" onClick={() => {
                if (!yamlDraft || typeof yamlDraft !== "string") return;
                try {
                  const cfg = yaml.load(yamlDraft);
                  if (!cfg || typeof cfg !== "object") { onYamlChange(yamlDraft + "\nfinal_output:\n  - name: initial\n    export: result.pdf\n"); return; }
                  const steps: Record<string, unknown>[] = Array.isArray((cfg as Record<string, unknown>).steps) ? (cfg as Record<string, unknown>).steps as Record<string, unknown>[] : [];
                  const finals: Record<string, unknown>[] = Array.isArray((cfg as Record<string, unknown>).final_output) ? (cfg as Record<string, unknown>).final_output as Record<string, unknown>[] : [];
                  const lastIdx = steps.length;
                  const lastVar = lastIdx > 0 ? String(steps[lastIdx - 1]?.output || `step_${lastIdx}`) : "initial";
                  finals.push({ name: lastVar, export: `${lastVar}.pdf` });
                  const out = yaml.dump(cfg, { indent: 2, noRefs: true, sortKeys: false });
                  if (out) onYamlChange(out); else { console.warn("yaml.dump returned empty"); }
                } catch (e) { console.error("Add output failed:", e); }
              }}>+ Add output</button>
            </div>
          )}
          <details>
            <summary><Layers size={15} /> Technical details</summary>
            <JsonBlock title="Stage configuration" value={selectedNode.meta} />
            {pipeline?.globals && Object.keys(pipeline.globals).length > 0 && (
              <JsonBlock title="Style/global variables" value={pipeline.globals} />
            )}
          </details>
          <details className="advanced-block version-history" open={Boolean(focusedVersionId)}>
            <summary><RefreshCw size={15} /> Versions</summary>
            <div className="version-list">
              {versions.length ? versions.slice(0, 8).map((version) => (
                <div key={version.id} className={`version-row ${version.status === "active" ? "active" : ""} ${focusedVersionId === version.id ? "focused" : ""}`}>
                  <strong>#{version.id}</strong>
                  <span>{version.exportName || version.configFile || "pipeline"}</span>
                  <small>{shortDate(version.timestamp)}</small>
                  {version.status !== "active" && (
                    <button className="version-restore-button" onClick={() => onSwitchVersion(version)}>
                      Restore
                    </button>
                  )}
                </div>
              )) : (
                <div className="friendly-empty">
                  <strong>No versions yet.</strong>
                  <span>Run a pipeline to create recoverable results.</span>
                </div>
              )}
            </div>
          </details>
          <details>
            <summary><FileCode size={15} /> Advanced YAML {yamlDirty ? " -  unsaved" : ""}</summary>
            <textarea
              className="yaml-editor"
              value={yamlDraft}
              spellCheck={false}
              onChange={(event) => onYamlChange(event.target.value)}
            />
            <div className="editor-actions">
              <button disabled={!yamlDirty || saveBusy} onClick={onSave}>
                {saveBusy ? "Saving" : "Save YAML"}
              </button>
              <button disabled={!yamlDirty || saveBusy} onClick={() => onYamlChange(pipeline?.yaml || "")}>
                Reset
              </button>
            </div>
          </details>
          {runResult && (
            runResult.ok ? (
              <div className="run-feedback success">
                <div className="run-feedback-title">
                  <CheckCircle size={16} />
                  <strong>{runResult.forceFresh ? "Fresh run finished" : "Run finished"}</strong>
                </div>
                <p>{runResult.summary}</p>
              </div>
            ) : (
              <div className="run-feedback failed">
                <div className="run-feedback-title">
                  <Activity size={16} />
                  <strong>{runResult.errorType || "ExecutionError"}</strong>
                </div>
                <p>{runResult.summary}</p>
                {(runResult.failedProcessor || runResult.failedStage) && (
                  <div className="run-feedback-grid">
                    {runResult.failedProcessor && (
                      <div>
                        <span>Processor</span>
                        <strong>{runResult.failedProcessor}</strong>
                      </div>
                    )}
                    {runResult.failedStage && (
                      <div>
                        <span>Stage</span>
                        <strong>{runResult.failedStage}</strong>
                      </div>
                    )}
                  </div>
                )}
                {runResult.hint && <small>{runResult.hint}</small>}
                <details>
                  <summary><FileCode size={15} /> Raw execution log</summary>
                  <div className="run-status failed">exit code {runResult.returnCode}</div>
                  <pre className="code-block">{runResult.stdout || runResult.stderr || "No output"}</pre>
                  {runResult.stderr && <pre className="code-block error-output">{runResult.stderr}</pre>}
                </details>
              </div>
            )
          )}
        </div>
      ) : (
        <div className="empty-state">Click a source, processor, or export node to inspect it.</div>
      )}
    </section>
  );
}

function StagePreviewModal({
  experiment,
  entry,
  title,
  preview,
  onClose
}: {
  experiment: string;
  entry: Entry;
  title: string;
  preview: Preview;
  onClose: () => void;
}) {
  return (
    <div className="preview-modal-backdrop" role="dialog" aria-modal="true" aria-label={title}>
      <section className="preview-modal">
        <div className="preview-modal-header">
          <div>
            <h3>{title}</h3>
            <p>ID {entry.id}  -  {entry.extension || entry.type}</p>
          </div>
          <div className="preview-modal-actions">
            <a className="download-link" href={api.fileUrl(experiment, entry.id)} target="_blank" rel="noreferrer">
              <Download size={15} /> Open file
            </a>
            <button className="icon-action" onClick={onClose} title="Close preview">
              <X size={16} />
            </button>
          </div>
        </div>
        <div className="preview-modal-body">
          {preview.kind === "table" && <TablePreview preview={preview} />}
          {preview.kind === "text" && <pre className="text-preview modal-text">{preview.text}</pre>}
          {preview.kind === "file" && entry.extension === ".pdf" && (
            <PdfViewer url={api.fileUrl(experiment, entry.id)} title={title} />
          )}
          {preview.kind === "file" && [".png", ".jpg", ".jpeg", ".svg"].includes(entry.extension) && (
            <img className="modal-image-preview" src={api.fileUrl(experiment, entry.id)} alt={title} />
          )}
          {preview.kind === "file" && ![".pdf", ".png", ".jpg", ".jpeg", ".svg"].includes(entry.extension) && (
            <div className="empty-state">Preview is not available for this file type. Open the file directly.</div>
          )}
        </div>
      </section>
    </div>
  );
}

function StoryView({
  experiment,
  pipeline,
  liveGraph,
  stageResults,
  selectedNode,
  onSelectNode,
  finalEntry,
  sourceSummary,
  onOpenResults
}: {
  experiment: string;
  pipeline: PipelineDetail | null;
  liveGraph: { nodes: GraphNode[]; edges: Array<{ id: string; source: string; target: string; label: string }> };
  stageResults: StageResults;
  selectedNode: GraphNode | null;
  onSelectNode: (node: GraphNode) => void;
  finalEntry: Entry | null;
  sourceSummary: string;
  onOpenResults: (entry: Entry) => void;
}) {
  if (!pipeline) {
    return <div className="empty-state">Choose a pipeline to see a readable processing story.</div>;
  }
  const gr = liveGraph.nodes.length ? liveGraph : pipeline.graph;
  const sources = gr.nodes.filter((node) => node.type === "source");
  const steps = gr.nodes.filter((node) => node.type === "processor");
  const outputs = gr.nodes.filter((node) => node.type === "export");
  return (
    <div className="story-view">
      <div className="story-intro">
        <div>
          <h3>{pipeline.name}</h3>
          <p>{pipeline.stepCount} processing steps  -  {pipeline.exportCount} final outputs</p>
        </div>
        <span>{pipeline.group}</span>
      </div>

      <FinalOutputSpotlight experiment={experiment} entry={finalEntry} exportLabel={outputs.find(n => (stageResults[n.id] || []).some(e => e.id === finalEntry?.id))?.label || ""} sourceSummary={sourceSummary} onOpenResults={onOpenResults} />

      <div className="story-section">
        <div className="story-section-title">1. Choose input data</div>
        {sources.map((node) => (
          <StoryCard key={node.id} indexLabel="Input" node={node} selected={selectedNode?.id === node.id} resultCount={(stageResults[node.id] || []).length} onClick={() => onSelectNode(node)} />
        ))}
      </div>

      <div className="story-section">
        <div className="story-section-title">2. Process and shape the data</div>
        {steps.map((node, index) => (
          <StoryCard key={node.id} indexLabel={`Step ${index + 1}`} node={node} selected={selectedNode?.id === node.id} resultCount={(stageResults[node.id] || []).length} onClick={() => onSelectNode(node)} />
        ))}
      </div>

      {outputs.length > 0 && (
        <div className="story-section">
          <div className="story-section-title">3. Save final results</div>
          {outputs.map((node) => (
            <StoryCard key={node.id} indexLabel="Output" node={node} selected={selectedNode?.id === node.id} resultCount={(stageResults[node.id] || []).length} onClick={() => onSelectNode(node)} />
          ))}
        </div>
      )}
    </div>
  );
}

function StoryCard({
  indexLabel,
  node,
  selected,
  resultCount,
  onClick
}: {
  indexLabel: string;
  node: GraphNode;
  selected: boolean;
  resultCount: number;
  onClick: () => void;
}) {
  return (
    <button className={`story-card ${selected ? "active" : ""}`} onClick={onClick}>
      <div className="story-card-index">{indexLabel}</div>
      <div className="story-card-main">
        <strong>{node.label}</strong>
        <span>{describeNode(node)}</span>
      </div>
      <div className="story-card-status">
        <CheckCircle size={15} />
        {resultCount} result{resultCount === 1 ? "" : "s"}
      </div>
    </button>
  );
}

function FinalOutputSpotlight({
  experiment,
  entry,
  exportLabel,
  sourceSummary,
  onOpenResults
}: {
  experiment: string;
  entry: Entry | null;
  exportLabel: string;
  sourceSummary: string;
  onOpenResults: (entry: Entry) => void;
}) {
  const [preview, setPreview] = useState<Preview | null>(null);
  const [previewError, setPreviewError] = useState(false);

  useEffect(() => {
    if (!experiment || !entry) {
      setPreview(null);
      setPreviewError(false);
      return;
    }
    let cancelled = false;
    setPreview(null);
    setPreviewError(false);
    api.preview(experiment, entry.id)
      .then((data) => {
        if (!cancelled) setPreview(data);
      })
      .catch(() => {
        if (!cancelled) setPreviewError(true);
      });
    return () => {
      cancelled = true;
    };
  }, [experiment, entry?.id]);

  if (!entry) {
    return (
      <section className="final-output-spotlight empty">
        <div className="final-output-copy">
          <span className="final-output-kicker">Final output</span>
          <strong>No synced final output yet</strong>
          <p>Run this pipeline to generate and sync the latest export here.</p>
        </div>
      </section>
    );
  }

  return (
    <section className="final-output-spotlight">
      <div className="final-output-copy">
        <span className="final-output-kicker">Final output</span>
        <strong>{exportLabel || entry.fileName || `Entry ${entry.id}`}</strong>
        <div className="final-output-meta">
          <span>{entry.fileName || `Entry ${entry.id}`}</span>
          <span>{sourceSummary}</span>
          <span>{shortDate(entry.timestamp)}</span>
        </div>
        <details className="technical-details">
          <summary>Technical details</summary>
          <p>{entry.description || "No processor details recorded."}</p>
        </details>
        <div className="final-output-actions">
          <a className="hero-secondary" href={api.fileUrl(experiment, entry.id)} target="_blank" rel="noreferrer">
            <Download size={15} /> Open file
          </a>
          <button className="compact-action" onClick={() => onOpenResults(entry)}>
            <Eye size={15} /> Open in Data
          </button>
        </div>
      </div>
      <div className="final-output-preview">
        {preview?.kind === "file" && entry.extension === ".pdf" && (
          <PdfViewer url={api.fileUrl(experiment, entry.id)} title={entry.fileName} />
        )}
        {preview?.kind === "file" && [".png", ".jpg", ".jpeg", ".svg"].includes(entry.extension) && (
          <img className="final-output-image" src={api.fileUrl(experiment, entry.id)} alt={entry.fileName} />
        )}
        {preview?.kind === "table" && <CompactTablePreview preview={preview} />}
        {preview?.kind === "text" && <pre className="text-preview compact">{preview.text.slice(0, 2000)}</pre>}
        {!preview && !previewError && <div className="friendly-empty"><strong>Loading final output...</strong><span>Fetching the latest exported result for this pipeline.</span></div>}
        {previewError && <div className="friendly-empty"><strong>Preview unavailable</strong><span>The result file could not be loaded. Check that the entry exists in this experiment.</span></div>}
      </div>
    </section>
  );
}

function describeNode(node: GraphNode) {
  const meta = (node.meta || {}) as Record<string, unknown>;
  if (node.type === "source") {
    const includes = meta.includes as Array<Record<string, unknown>> | undefined;
    return `${includes?.length || 0} file pattern${includes?.length === 1 ? "" : "s"} loaded into the run`;
  }
  if (node.type === "export") {
    return `Final named output: ${node.subtitle}`;
  }
  const input = String(meta.inputs ?? "initial");
  const output = meta.outputs ? `${Object.keys(meta.outputs as Record<string, unknown>).length} branches` : String(meta.output ?? node.subtitle);
  const paramCount = Object.keys((meta.params as Record<string, unknown> | undefined) || {}).length;
  return `Takes ${input}, produces ${output}${paramCount ? `, with ${paramCount} tuned parameter${paramCount === 1 ? "" : "s"}` : ""}`;
}

function friendlyStageType(node: GraphNode) {
  if (node.type === "source") return "Input data";
  if (node.type === "export") return "Final output";
  return "Processing stage";
}

function ParamsEditor({ node, yamlDraft, onYamlChange, processors }: { node: GraphNode; yamlDraft: string; onYamlChange: (value: string) => void; processors?: Processor[] }) {
  const parsed = useMemo(() => {
    try { return yaml.load(yamlDraft) as Record<string, unknown>; } catch { return {}; }
  }, [yamlDraft]);
  const meta = (node.meta || {}) as Record<string, unknown>;

  // Conditional rendering after hooks.
  const updateSource = useCallback((idx: number, key: string, value: unknown) => {
    try {
      const config = yaml.load(yamlDraft) as Record<string, unknown>;
      const list = (((config?.initial_load as Record<string, unknown>)?.include) as Array<Record<string, unknown>>) || [];
      if (idx >= 0 && idx < list.length) { list[idx][key] = value; onYamlChange(yaml.dump(config, { indent: 2, noRefs: true, sortKeys: false })); }
    } catch {}
  }, [yamlDraft, onYamlChange]);

  const exportMeta = meta.export as string | undefined;
  const nameMeta = meta.name as string | undefined;
  const updateExport = useCallback((key: string, value: unknown) => {
    try {
      const config = yaml.load(yamlDraft) as Record<string, unknown>;
      const finals = (config?.final_output || []) as Array<Record<string, unknown>>;
      const idx = finals.findIndex((fo) => fo.name === nameMeta || fo.export === exportMeta);
      if (idx >= 0) { finals[idx][key] = value; onYamlChange(yaml.dump(config, { indent: 2, noRefs: true, sortKeys: false })); }
    } catch {}
  }, [yamlDraft, onYamlChange, nameMeta, exportMeta]);

  const deleteFinalOutput = useCallback(() => {
    try {
      const config = yaml.load(yamlDraft) as Record<string, unknown>;
      const finals = (config?.final_output || []) as Array<Record<string, unknown>>;
      const idx = finals.findIndex((fo) => fo.name === nameMeta || fo.export === exportMeta);
      if (idx >= 0) { finals.splice(idx, 1); onYamlChange(yaml.dump(config, { indent: 2, noRefs: true, sortKeys: false })); }
    } catch {}
  }, [yamlDraft, onYamlChange, nameMeta, exportMeta]);

  const index = meta.index as number | undefined;
  const stepFromYaml = index && index > 0
    ? ((parsed?.steps || []) as Record<string, unknown>[])[index - 1] as Record<string, unknown> | undefined
    : undefined;
  const params = (stepFromYaml?.params as Record<string, unknown>) || {};
  const curProcName = (stepFromYaml?.processor as string) || "";
  const procSig = processors?.find(p => p.name === curProcName)?.signature || [];
  const paramEntries = Object.entries(params).filter(([key]) => key !== "kwargs");
  const optionalParamEntries = procSig.filter((param) => param.name !== "kwargs" && !(param.name in params));
  // Keep params that exist in YAML even if they are not in the processor signature.

  const patchStep = useCallback((patch: Record<string, unknown>) => {
    try {
      const config = yaml.load(yamlDraft) as Record<string, unknown>;
      const steps = (config?.steps || []) as Record<string, unknown>[];
      if (index && index > 0 && index <= steps.length) { Object.assign(steps[index - 1], patch); onYamlChange(yaml.dump(config, { indent: 2, noRefs: true, sortKeys: false })); }
    } catch {}
  }, [yamlDraft, onYamlChange, index]);

  const patchParam = useCallback((key: string, value: unknown) => {
    const next = { ...params };
    next[key] = value;
    patchStep({ params: next });
  }, [params, patchStep]);

  const removeParam = useCallback((key: string) => {
    const next = { ...params };
    delete next[key];
    patchStep({ params: next });
  }, [params, patchStep]);

  const deleteStep = useCallback(() => {
    try {
      const config = yaml.load(yamlDraft) as Record<string, unknown>;
      const steps = (config?.steps || []) as YamlStep[];
      if (index && index > 0 && index <= steps.length) {
        const removed = steps[index - 1];
        const removedOutputs = new Set(stepOutputVars(removed, index));
        const replacement = removed.inputs || "initial";
        steps.splice(index - 1, 1);
        for (let i = index - 1; i < steps.length; i++) {
          steps[i].inputs = replaceInputRefs(steps[i].inputs, removedOutputs, replacement);
        }
        const finals = (config?.final_output || []) as YamlStep[];
        replaceFinalRefs(finals, removedOutputs, replacement);
        onYamlChange(yaml.dump(config, { indent: 2, noRefs: true, sortKeys: false }));
      }
    } catch {}
  }, [yamlDraft, onYamlChange, index]);

  const changeProcessor = useCallback((processorName: string) => {
    patchStep({
      processor: processorName,
      params: defaultParamsForProcessor(processors, processorName)
    });
  }, [patchStep, processors]);

  const renameOutput = useCallback((value: unknown) => {
    try {
      const config = yaml.load(yamlDraft) as Record<string, unknown>;
      const steps = (config?.steps || []) as YamlStep[];
      if (index && index > 0 && index <= steps.length) {
        const step = steps[index - 1];
        const oldOutputs = new Set(stepOutputVars(step, index));
        const newOutput = String(value || `step_${index}`);
        step.output = newOutput;
        delete step.outputs;
        for (let i = index; i < steps.length; i++) {
          steps[i].inputs = replaceInputRefs(steps[i].inputs, oldOutputs, newOutput);
        }
        const finals = (config?.final_output || []) as YamlStep[];
        replaceFinalRefs(finals, oldOutputs, newOutput);
        onYamlChange(yaml.dump(config, { indent: 2, noRefs: true, sortKeys: false }));
      }
    } catch {}
  }, [yamlDraft, onYamlChange, index]);

  // Conditional rendering after hooks.
  if (node.type === "source") {
    const incList = (((parsed?.initial_load as Record<string, unknown>)?.include) as Array<Record<string, unknown>>) || [];
    if (incList.length === 0) return null;
    return (
      <details className="params-editor">
        <summary><Database size={15} /> Data sources</summary>
        <div className="params-grid">
          {incList.map((inc, i) => (
            <div key={i} className="source-param-group">
              <strong className="source-param-label">Include #{i + 1}</strong>
              {Object.entries(inc).map(([key, value]) => (
                <ParamField key={key} name={key} value={value} onChange={(v) => updateSource(i, key, v)} />
              ))}
            </div>
          ))}
        </div>
      </details>
    );
  }

  if (node.type === "export") {
    return (
      <>
        <details className="params-editor">
          <summary><Download size={15} /> Export config</summary>
          <div className="params-grid">
            <ParamField name="name" value={nameMeta ?? ""} onChange={(v) => updateExport("name", v)} />
            <ParamField name="export" value={exportMeta ?? ""} onChange={(v) => updateExport("export", v)} />
          </div>
          <button className="danger-button" onClick={deleteFinalOutput} style={{ margin: "4px 12px 12px" }}>
            Delete this output
          </button>
        </details>
      </>
    );
  }

  // Note: comment text was normalized to avoid mojibake.
  const rawInputs = stepFromYaml?.inputs;
  const inputsStr = Array.isArray(rawInputs) ? rawInputs.join(", ") : (rawInputs as string) || "";
  const curProc = (stepFromYaml?.processor as string) || "";
  return (
    <>
      <details className="params-editor">
        <summary><SlidersHorizontal size={15} /> Step config</summary>
        <div className="params-grid">
          <ProcessorChooser processors={processors || []} value={curProc} onChange={changeProcessor} />
          <ParamField name="inputs" value={inputsStr} onChange={(v) => patchStep({ inputs: String(v).includes(",") ? String(v).split(",").map(s => s.trim()) : String(v) })} />
          <ParamField name="output" value={(stepFromYaml?.output as string) || ""} onChange={renameOutput} />
          {paramEntries.map(([key, value]) => (
            <ParamField key={key} name={key} value={value} processorName={curProc} onChange={(v) => patchParam(key, v)} onRemove={() => removeParam(key)} />
          ))}
          {optionalParamEntries.length > 0 && (
            <details className="optional-param-picker">
              <summary>Add parameter</summary>
              <div>
                {optionalParamEntries.map((param) => (
                  <button key={param.name} onClick={() => patchParam(param.name, param.default ?? null)}>
                    {param.name}
                  </button>
                ))}
              </div>
            </details>
          )}
        </div>
        <button className="danger-button" onClick={deleteStep} style={{ margin: "4px 12px 12px" }}>
          Delete this step
        </button>
      </details>
    </>
  );
}

function ProcessorChooser({
  processors,
  value,
  onChange
}: {
  processors: Processor[];
  value: string;
  onChange: (processorName: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [pickerOpen, setPickerOpen] = useState(false);
  const uniqueProcessors = useMemo(() => {
    const byName = new Map<string, Processor>();
    for (const processor of processors) {
      if (processor.name && !byName.has(processor.name)) byName.set(processor.name, processor);
    }
    return [...byName.values()].sort((a, b) => a.name.localeCompare(b.name));
  }, [processors]);
  const current = uniqueProcessors.find((processor) => processor.name === value);
  const visible = useMemo(() => {
    const needle = query.trim().toLowerCase();
    const filtered = needle
      ? uniqueProcessors.filter((processor) => `${processor.name} ${processor.description} ${processor.category} ${processor.outputExt}`.toLowerCase().includes(needle))
      : uniqueProcessors;
    const preferred = ["Plot", "Table", "Parse", "File", "Custom"];
    return [...filtered]
      .sort((a, b) => {
        const ai = preferred.indexOf(processorGroup(a));
        const bi = preferred.indexOf(processorGroup(b));
        return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi) || a.name.localeCompare(b.name);
      })
      .slice(0, 18);
  }, [query, uniqueProcessors]);

  const grouped = useMemo(() => {
    const groups = new Map<string, Processor[]>();
    for (const processor of visible) {
      const group = processorGroup(processor);
      groups.set(group, [...(groups.get(group) || []), processor]);
    }
    return [...groups.entries()];
  }, [visible]);

  return (
    <div className={`processor-chooser ${pickerOpen ? "expanded" : ""}`}>
      <label className="param-field">
        <span>processor</span>
        <select value={value} onChange={(event) => onChange(event.target.value)}>
          {!value && <option value="">Select...</option>}
          {uniqueProcessors.map((processor) => (
            <option key={processor.name} value={processor.name}>
              {processor.name}
            </option>
          ))}
        </select>
      </label>

      {current && (
        <div className="current-processor-card">
          <div>
            <strong>{current.name}</strong>
            <span>{processorSummary(current)}</span>
            <button
              className="processor-change-button"
              onClick={() => {
                setPickerOpen((open) => !open);
                if (pickerOpen) setQuery("");
              }}
            >
              {pickerOpen ? <X size={14} /> : <Search size={14} />}
              {pickerOpen ? "Hide choices" : "Change processor"}
            </button>
          </div>
          <small>{current.inputType} input  -  {current.outputType} output  -  {current.outputExt || "same type"}</small>
        </div>
      )}

      <div className="processor-search">
        <Search size={15} />
        <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search processors" />
      </div>

      <div className="processor-choice-groups">
        {grouped.map(([group, items]) => (
          <div key={group} className="processor-choice-group">
            <span>{group}</span>
            <div className="processor-choice-list">
              {items.map((processor) => (
                <button
                  key={processor.name}
                  className={`processor-choice ${processor.name === value ? "active" : ""}`}
                  onClick={() => {
                    onChange(processor.name);
                    setPickerOpen(false);
                    setQuery("");
                  }}
                >
                  <strong>{processor.name}</strong>
                  <small>{processorSummary(processor)}</small>
                </button>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function formatParamDraft(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

function parseParamDraft(text: string): unknown {
  const trimmed = text.trim();
  if (!trimmed) return "";
  try {
    return yaml.load(trimmed);
  } catch {
    return text;
  }
}

function paramGenericHint(name: string, value: unknown) {
  const lowered = name.toLowerCase();
  if (typeof value === "boolean") return "Toggle true/false.";
  if (typeof value === "number") return "Number.";
  if (Array.isArray(value) || /(lim|range|figsize|ticks|order|cols|colors|styles|labels)/.test(lowered)) {
    return "YAML value. Lists use [a, b]; objects use {key: value}.";
  }
  if (value && typeof value === "object") return "YAML object, for example {raw: Display} or multi-line YAML.";
  return "YAML value: text, number, true/false, null, [a, b], or {key: value}.";
}

function paramSemanticHint(processorName: string | undefined, name: string) {
  const processor = (processorName || "").toLowerCase();
  const param = name.toLowerCase();
  const isLine = processor === "plot_line";
  const isBar = processor === "plot_bar";
  const isGroupedBar = processor === "plot_grouped_bar";
  const isHorizontalBar = processor === "plot_horizontal_bar" || processor === "plot_hbar" || processor.includes("hbar");
  const isDualAxis = processor === "plot_dual_axis_line";
  const isTimeline = processor === "plot_timeline_hbar" || processor.includes("timeline");

  if (param === "figsize") return "Figure size in inches: [width, height]. Example: [10, 6].";
  if (param === "xlim") {
    if (isLine || isDualAxis) return "X-axis data range: [min, max]. Example [0, 100] shows the continuous x range from 0 to 100.";
    if (isBar || isGroupedBar) return "Category layout coordinate range, not a data-value filter. Usually leave unset; use ylim to limit bar values.";
    if (isHorizontalBar) return "Numeric value-axis range for horizontal bars: [min, max].";
    if (isTimeline) return "This processor does not define xlim; time is normalized from the first start value.";
  }
  if (param === "ylim") {
    if (isLine || isBar) return "Y-axis numeric value range: [min, max]. Example [0, 100] limits visible values to 0 through 100.";
    if (isGroupedBar) return "Y-axis numeric value range. With row/column facets, an object can set per-subplot ranges.";
    if (isHorizontalBar || isTimeline) return "Vertical category/lane position range, not the numeric value range. Usually leave unset; use xlim for horizontal value range.";
  }
  if (param === "ylim_y1") return "Left Y-axis numeric range: [min, max].";
  if (param === "ylim_y2") return "Right Y-axis numeric range: [min, max].";
  if (param === "xticks_num" || param === "yticks_num" || param === "yticks_num_y1" || param === "yticks_num_y2") {
    return "Approximate maximum tick count used by Matplotlib MaxNLocator; this is not a list of tick values.";
  }
  if (param.endsWith("_col") || param.endsWith("_cols") || ["time_col", "value_col", "value_cols_y1", "value_cols_y2", "main_group_col", "sub_group_col", "category_col", "sub_category_col", "start_col", "end_col"].includes(param)) {
    return param.endsWith("cols") || param.startsWith("value_cols")
      ? "List of data column names. Example: [loss, accuracy]."
      : "Data column name. It must match a column in the selected table.";
  }
  if (param.endsWith("_order")) return "Exact category order list. Values must match the data, for example [baseline, optimized].";
  if (param.endsWith("_labels") || param.endsWith("_map")) return "Mapping from raw data values to display labels. Example: {baseline: Baseline}.";
  if (["colors", "colors_y1", "colors_y2", "hatches", "line_styles_y1", "line_styles_y2"].includes(param)) {
    return "Style list cycles by series/group; an object maps a series or group name to a style.";
  }
  if (param === "aggregate_func") return "Aggregation for duplicate groups before plotting, such as mean, median, max, or min.";
  if (param === "normalization") return "Object config for grouped-bar normalization; enable it only when a reference condition is defined.";
  return "";
}

function paramFormatHint(name: string, value: unknown, processorName?: string) {
  const semantic = paramSemanticHint(processorName, name);
  const generic = paramGenericHint(name, value);
  return semantic ? `${semantic} ${generic}` : generic;
}

function FieldRemoveButton({ onRemove }: { onRemove?: () => void }) {
  if (!onRemove) return null;
  return (
    <button className="param-remove-button" type="button" onClick={onRemove} title="Remove parameter">
      <X size={13} />
    </button>
  );
}

function ParamField({
  name,
  value,
  processorName,
  onChange,
  onRemove
}: {
  name: string;
  value: unknown;
  processorName?: string;
  onChange: (v: unknown) => void;
  onRemove?: () => void;
}) {
  const [draft, setDraft] = useState(() => formatParamDraft(value));
  useEffect(() => {
    setDraft(formatParamDraft(value));
  }, [value]);
  const commitDraft = () => onChange(parseParamDraft(draft));
  const hint = paramFormatHint(name, value, processorName);

  if (value === null || value === undefined) {
    return (
      <label className="param-field">
        <span>{name}</span>
        <input type="text" value={draft} placeholder="null" onChange={(e) => setDraft(e.target.value)} onBlur={commitDraft} onKeyDown={(e) => { if (e.key === "Enter") commitDraft(); }} />
        <small>{hint}</small>
        <FieldRemoveButton onRemove={onRemove} />
      </label>
    );
  }
  if (typeof value === "boolean") {
    return (
      <label className="param-field param-field-checkbox">
        <input type="checkbox" checked={value} onChange={(e) => onChange(e.target.checked)} />
        <span>{name}</span>
        <small>{hint}</small>
        <FieldRemoveButton onRemove={onRemove} />
      </label>
    );
  }
  if (typeof value === "number") {
    return (
      <label className="param-field">
        <span>{name}</span>
        <input type="number" value={value} step={Number.isInteger(value) ? 1 : 0.1}
          onChange={(e) => onChange(e.target.value ? (e.target.value.includes(".") ? parseFloat(e.target.value) : parseInt(e.target.value, 10)) : 0)} />
        <small>{hint}</small>
        <FieldRemoveButton onRemove={onRemove} />
      </label>
    );
  }
  if (Array.isArray(value)) {
    return (
      <label className="param-field">
        <span>{name}</span>
        <input type="text" value={draft} placeholder="[val1, val2, ...]" onChange={(e) => setDraft(e.target.value)} onBlur={commitDraft} onKeyDown={(e) => { if (e.key === "Enter") commitDraft(); }} />
        <small>{hint}</small>
        <FieldRemoveButton onRemove={onRemove} />
      </label>
    );
  }
  if (typeof value === "object") {
    return (
      <label className="param-field param-field-block">
        <span>{name}</span>
        <textarea rows={3} value={draft} onChange={(e) => setDraft(e.target.value)} onBlur={commitDraft} />
        <small>{hint}</small>
        <FieldRemoveButton onRemove={onRemove} />
      </label>
    );
  }
  return (
    <label className="param-field">
      <span>{name}</span>
      <input type="text" value={draft} onChange={(e) => setDraft(e.target.value)} onBlur={commitDraft} onKeyDown={(e) => { if (e.key === "Enter") commitDraft(); }} />
      <small>{hint}</small>
      <FieldRemoveButton onRemove={onRemove} />
    </label>
  );
}

function StageSummary({ node, resultCount }: { node: GraphNode; resultCount: number }) {
  const meta = (node.meta || {}) as Record<string, unknown>;
  const inputs = String(meta.inputs ?? "initial");
  const namedOutputs = meta.outputs as Record<string, unknown> | undefined;
  const output = namedOutputs && Object.keys(namedOutputs).length
    ? `${Object.keys(meta.outputs as Record<string, unknown>).length} branches`
    : String(meta.output ?? node.subtitle);
  return (
    <div className="stage-summary">
      <div>
        <span>Input</span>
        <strong>{inputs}</strong>
      </div>
      <div>
        <span>Output</span>
        <strong>{output}</strong>
      </div>
      <div>
        <span>Saved results</span>
        <strong>{resultCount}</strong>
      </div>
    </div>
  );
}

function CompactTablePreview({ preview }: { preview: Extract<Preview, { kind: "table" }> }) {
  const columns = preview.columns.slice(0, 6);
  return (
    <div className="compact-table-preview">
      <p>{preview.shape[0]} rows x {preview.shape[1]} columns</p>
      <table>
        <thead>
          <tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {preview.rows.slice(0, 12).map((row, index) => (
            <tr key={index}>
              {columns.map((column) => <td key={column}>{String(row[column] ?? "")}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function dataGroupLabel(entry: Entry) {
  const ext = entry.extension.toLowerCase();
  const text = `${entry.type} ${entry.tags.join(" ")} ${entry.fileName}`.toLowerCase();
  if (entry.type === "raw" || text.includes("raw") || text.includes("input")) return "Source inputs";
  if ([".pdf", ".png", ".jpg", ".jpeg", ".svg"].includes(ext) || text.includes("export")) return "Final outputs";
  if (entry.type === "processed" || [".parquet", ".csv", ".tsv", ".xlsx", ".xls", ".json", ".jsonl"].includes(ext)) return "Intermediate results";
  return "Other files";
}

const dataGroupOrder = ["Final outputs", "Intermediate results", "Source inputs", "Other files"];

function defaultOpenDataGroups() {
  return new Set(["Final outputs", "Other files"]);
}

type FigureReturnTarget = {
  entry: Entry;
  resultName: string;
  pipelineName: string;
  timestamp: string;
};

type FigureVersionOption = {
  version: Version;
  entry: Entry;
  resultName: string;
  pipelineName: string;
};

function DataView({
  experiment,
  entries,
  exports,
  versions,
  selectedEntry,
  setSelectedEntry,
  preview,
  lineage,
  setView,
  onOpenPipelineVersion,
  onSwitchPipelineVersion
}: {
  experiment: string;
  entries: Entry[];
  exports: ExportItem[];
  versions: Version[];
  selectedEntry: Entry | null;
  setSelectedEntry: (entry: Entry) => void;
  preview: Preview | null;
  lineage: Lineage | null;
  setView: (view: View) => void;
  onOpenPipelineVersion: (version: Version) => void;
  onSwitchPipelineVersion: (version: Version) => void;
}) {
  const [filter, setFilter] = useState("");
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(defaultOpenDataGroups);
  const [focusedEntryId, setFocusedEntryId] = useState<number | null>(null);
  const [lineageReturnTarget, setLineageReturnTarget] = useState<FigureReturnTarget | null>(null);
  const [comparisonVersionIds, setComparisonVersionIds] = useState<number[]>([]);
  const [compareOpen, setCompareOpen] = useState(false);
  const currentExportIds = useMemo(() => new Set(exports.map((item) => item.entryId)), [exports]);
  const versionExportIds = useMemo(() => new Set(versions.map((version) => version.exportId).filter((id): id is number => Boolean(id))), [versions]);
  const filtered = entries.filter((entry) => {
    const matchesFilter = `${entry.fileName} ${entry.tags.join(" ")} ${entry.type}`.toLowerCase().includes(filter.toLowerCase());
    if (!matchesFilter) return false;
    if (dataGroupLabel(entry) === "Final outputs" && versionExportIds.has(entry.id) && !currentExportIds.has(entry.id)) {
      return false;
    }
    return true;
  });
  const figureContextByEntryId = useMemo(() => {
    const result = new Map<number, { resultName: string; pipelineName: string }>();
    for (const version of versions) {
      if (version.exportId) {
        result.set(version.exportId, {
          resultName: version.exportName || `Entry ${version.exportId}`,
          pipelineName: version.pipelinePath || version.configFile || "Unknown pipeline"
        });
      }
    }
    for (const item of exports) {
      if (!result.has(item.entryId)) {
        result.set(item.entryId, {
          resultName: item.name,
          pipelineName: "Named export"
        });
      }
    }
    return result;
  }, [exports, versions]);
  const returnTargetsByEntryId = useMemo(() => {
    const exportEntryById = new Map<number, Entry>();
    const exportNameById = new Map<number, string>();
    for (const item of exports) {
      exportEntryById.set(item.entryId, item.entry);
      exportNameById.set(item.entryId, item.name);
    }
    const result = new Map<number, FigureReturnTarget[]>();
    const seen = new Set<string>();
    for (const version of versions) {
      if (!version.exportId) continue;
      const finalEntry = exportEntryById.get(version.exportId) || entries.find((entry) => entry.id === version.exportId);
      if (!finalEntry) continue;
      const target: FigureReturnTarget = {
        entry: finalEntry,
        resultName: version.exportName || exportNameById.get(version.exportId) || finalEntry.fileName || `Entry ${version.exportId}`,
        pipelineName: version.pipelinePath || version.configFile || "Unknown pipeline",
        timestamp: version.timestamp
      };
      for (const entryId of version.entryIds) {
        const key = `${entryId}:${target.entry.id}`;
        if (seen.has(key)) continue;
        seen.add(key);
        result.set(entryId, [...(result.get(entryId) || []), target]);
      }
    }
    for (const [entryId, targets] of result.entries()) {
      targets.sort((a, b) => parseTimestamp(b.timestamp) - parseTimestamp(a.timestamp) || b.entry.id - a.entry.id);
      result.set(entryId, targets);
    }
    return result;
  }, [entries, exports, versions]);
  const figureVersions = useMemo(() => {
    if (!selectedEntry || dataGroupLabel(selectedEntry) !== "Final outputs") return [];
    const entryById = new Map(entries.map((entry) => [entry.id, entry]));
    const exportEntryById = new Map(exports.map((item) => [item.entryId, item.entry]));
    const exportNameById = new Map(exports.map((item) => [item.entryId, item.name]));
    const selectedVersion = versions.find((version) => version.exportId === selectedEntry.id);
    const selectedContext = figureContextByEntryId.get(selectedEntry.id);
    const selectedResultName = selectedVersion?.exportName || selectedContext?.resultName || exportNameById.get(selectedEntry.id) || selectedEntry.fileName;
    const selectedPipeline = selectedVersion?.pipelinePath || selectedVersion?.configFile || selectedContext?.pipelineName || "";
    const options: FigureVersionOption[] = [];
    for (const version of versions) {
      if (!version.exportId) continue;
      const versionResultName = version.exportName || exportNameById.get(version.exportId) || `Entry ${version.exportId}`;
      const sameResult = selectedResultName && versionResultName === selectedResultName;
      const samePipeline = selectedPipeline && (version.pipelinePath === selectedPipeline || version.configFile === selectedPipeline);
      if (version.exportId !== selectedEntry.id && !sameResult && !samePipeline) continue;
      const entry = exportEntryById.get(version.exportId) || entryById.get(version.exportId);
      if (!entry) continue;
      options.push({
        version,
        entry,
        resultName: versionResultName,
        pipelineName: version.pipelinePath || version.configFile || "Unknown pipeline"
      });
    }
    const seen = new Set<number>();
    return options
      .filter((option) => {
        if (seen.has(option.version.id)) return false;
        seen.add(option.version.id);
        return true;
      })
      .sort((a, b) => parseTimestamp(b.version.timestamp) - parseTimestamp(a.version.timestamp) || b.version.id - a.version.id);
  }, [entries, exports, figureContextByEntryId, selectedEntry, versions]);
  const activeFigureVersionId = figureVersions.find((option) => option.entry.id === selectedEntry?.id)?.version.id || null;
  const comparisonVersions = figureVersions.filter((option) => comparisonVersionIds.includes(option.version.id));
  const groupedEntries = filtered.reduce<Array<[string, Entry[]]>>((groups, entry) => {
    const label = dataGroupLabel(entry);
    const existing = groups.find(([groupLabel]) => groupLabel === label);
    if (existing) existing[1].push(entry);
    else groups.push([label, [entry]]);
    return groups;
  }, []).sort(([a], [b]) => dataGroupOrder.indexOf(a) - dataGroupOrder.indexOf(b));

  useEffect(() => {
    setExpandedGroups(defaultOpenDataGroups());
    setFocusedEntryId(null);
    setLineageReturnTarget(null);
  }, [experiment]);

  useEffect(() => {
    if (!selectedEntry) return;
    const group = dataGroupLabel(selectedEntry);
    setExpandedGroups((current) => new Set(current).add(group));
    setCompareOpen(false);
  }, [selectedEntry?.id]);

  useEffect(() => {
    setComparisonVersionIds((current) => current.filter((id) => figureVersions.some((option) => option.version.id === id)));
  }, [figureVersions]);

  function figureTargetForEntry(entry: Entry): FigureReturnTarget {
    const context = figureContextByEntryId.get(entry.id);
    return {
      entry,
      resultName: context?.resultName || entry.fileName || `Entry ${entry.id}`,
      pipelineName: context?.pipelineName || "Pipeline not recorded",
      timestamp: entry.timestamp
    };
  }

  const selectedFigureTarget = selectedEntry && dataGroupLabel(selectedEntry) === "Final outputs" ? figureTargetForEntry(selectedEntry) : null;
  const relatedFinalTargets = (() => {
    if (!selectedEntry || dataGroupLabel(selectedEntry) === "Final outputs") return [];
    const merged = [
      ...(lineageReturnTarget ? [lineageReturnTarget] : []),
      ...(returnTargetsByEntryId.get(selectedEntry.id) || [])
    ];
    const seen = new Set<number>();
    return merged.filter((target) => {
      if (target.entry.id === selectedEntry.id || seen.has(target.entry.id)) return false;
      seen.add(target.entry.id);
      return true;
    });
  })();

  function openEntry(entry: Entry, sourceFigure?: FigureReturnTarget | null) {
    const group = dataGroupLabel(entry);
    setFilter("");
    setExpandedGroups((current) => new Set(current).add(group));
    setSelectedEntry(entry);
    setFocusedEntryId(entry.id);
    setLineageReturnTarget(group === "Final outputs" ? null : sourceFigure || null);
    window.setTimeout(() => {
      document.getElementById(`data-entry-${entry.id}`)?.scrollIntoView({ block: "center", behavior: "smooth" });
    }, 80);
  }

  function toggleComparisonVersion(versionId: number) {
    setComparisonVersionIds((current) => {
      if (current.includes(versionId)) return current.filter((id) => id !== versionId);
      return [...current, versionId].slice(-4);
    });
  }

  function setGroupOpen(group: string, open: boolean) {
    setExpandedGroups((current) => {
      const next = new Set(current);
      if (open) next.add(group);
      else next.delete(group);
      return next;
    });
  }

  return (
    <div className="split-grid">
      <section className="panel">
        <div className="panel-header">
          <div>
            <h3>Outputs and files</h3>
            <p>Final outputs first; source inputs and intermediate results stay folded until needed</p>
          </div>
        </div>
        <div className="search-box">
          <Search size={15} />
          <input value={filter} onChange={(event) => setFilter(event.target.value)} placeholder="Filter by file, tag, type" />
        </div>
        <div className="entry-table">
          {filtered.length ? groupedEntries.map(([group, groupEntries]) => (
            <details
              className="entry-group"
              key={group}
              open={expandedGroups.has(group)}
              onToggle={(event) => setGroupOpen(group, event.currentTarget.open)}
            >
              <summary className="entry-group-title">
                <span>{group}</span>
                <small className="entry-group-count">{groupEntries.length}</small>
              </summary>
              {groupEntries.map((entry) => (
                <button
                  id={`data-entry-${entry.id}`}
                  key={entry.id}
                  className={`entry-row ${selectedEntry?.id === entry.id ? "active" : ""} ${focusedEntryId === entry.id ? "focused" : ""}`}
                  onClick={() => openEntry(entry, selectedFigureTarget)}
                >
                  <span className={`type-badge ${dataGroupLabel(entry) === "Final outputs" ? "output" : entry.type}`}>{dataGroupLabel(entry) === "Final outputs" ? "output" : entry.type}</span>
                  <span>
                    <strong>{figureContextByEntryId.get(entry.id)?.resultName || entry.fileName || `Entry ${entry.id}`}</strong>
                    {dataGroupLabel(entry) === "Final outputs" && (
                      <em>{figureContextByEntryId.get(entry.id)?.pipelineName || "Pipeline not recorded"}</em>
                    )}
                  </span>
                  <small>{formatBytes(entry.size)}</small>
                </button>
              ))}
            </details>
          )) : (
            <div className="friendly-empty">
              <strong>No data files found.</strong>
              <span>{entries.length ? "No files match the current filter." : "Upload data or add a source pattern before running a pipeline."}</span>
              <div className="empty-actions">
                <button className="compact-action" onClick={() => setView("create")}>
                  <Upload size={15} /> Add data
                </button>
              </div>
            </div>
          )}
        </div>
      </section>
      <PreviewPanel
        experiment={experiment}
        entry={selectedEntry}
        preview={preview}
        lineage={lineage}
        onOpenEntry={openEntry}
        sourceFigureTarget={selectedFigureTarget}
        returnTargets={relatedFinalTargets}
        figureVersions={figureVersions}
        activeFigureVersionId={activeFigureVersionId}
        comparisonVersionIds={comparisonVersionIds}
        comparisonVersions={comparisonVersions}
        onPreviewFigureVersion={(option) => openEntry(option.entry, null)}
        onToggleComparisonVersion={toggleComparisonVersion}
        onOpenCompare={() => setCompareOpen(true)}
        onOpenPipelineVersion={onOpenPipelineVersion}
        onSwitchPipelineVersion={onSwitchPipelineVersion}
      />
      {compareOpen && comparisonVersions.length >= 2 && (
        <VersionCompareModal
          experiment={experiment}
          versions={comparisonVersions}
          onClose={() => setCompareOpen(false)}
        />
      )}
    </div>
  );
}

function FigureVersionBrowser({
  versions,
  activeVersionId,
  comparisonVersionIds,
  comparisonCount,
  onPreviewVersion,
  onToggleComparison,
  onOpenCompare,
  onOpenPipelineVersion,
  onSwitchPipelineVersion
}: {
  versions: FigureVersionOption[];
  activeVersionId?: number | null;
  comparisonVersionIds: number[];
  comparisonCount: number;
  onPreviewVersion?: (option: FigureVersionOption) => void;
  onToggleComparison?: (versionId: number) => void;
  onOpenCompare?: () => void;
  onOpenPipelineVersion?: (version: Version) => void;
  onSwitchPipelineVersion?: (version: Version) => void;
}) {
  const activeIndex = Math.max(0, versions.findIndex((option) => option.version.id === activeVersionId));
  const active = versions[activeIndex] || versions[0];
  const newer = activeIndex > 0 ? versions[activeIndex - 1] : null;
  const older = activeIndex >= 0 && activeIndex < versions.length - 1 ? versions[activeIndex + 1] : null;
  return (
    <section className="figure-version-panel">
      <div className="figure-version-head">
        <div>
          <strong>Output versions</strong>
          <span>{versions.length} version{versions.length === 1 ? "" : "s"} for this output</span>
        </div>
        <div className="figure-version-actions">
          <button className="compare-button" disabled={comparisonCount < 2} onClick={onOpenCompare}>
            <GitBranch size={15} /> Compare selected
          </button>
          {active && (
            <button className="pipeline-jump-button" onClick={() => onOpenPipelineVersion?.(active.version)}>
              <GitBranch size={15} /> Open pipeline
            </button>
          )}
        </div>
      </div>
      {active && (
        <div className="version-nav-row">
          <button className="compact-action" disabled={!older} onClick={() => older && onPreviewVersion?.(older)}>
            <ChevronLeft size={15} /> Previous
          </button>
          <select
            value={active.version.id}
            onChange={(event) => {
              const next = versions.find((option) => option.version.id === Number(event.target.value));
              if (next) onPreviewVersion?.(next);
            }}
          >
            {versions.map((option, index) => (
              <option key={option.version.id} value={option.version.id}>
                v{versions.length - index} - #{option.version.id} - {shortDate(option.version.timestamp)}
              </option>
            ))}
          </select>
          <button className="compact-action" disabled={!newer} onClick={() => newer && onPreviewVersion?.(newer)}>
            Next <ChevronRight size={15} />
          </button>
        </div>
      )}
      <div className="figure-version-list">
        {versions.map((option, index) => {
          const selected = option.version.id === activeVersionId;
          const compared = comparisonVersionIds.includes(option.version.id);
          return (
            <div key={option.version.id} className={`figure-version-row ${selected ? "active" : ""}`}>
              <button className="figure-version-main" onClick={() => onPreviewVersion?.(option)}>
                <strong>v{versions.length - index} #{option.version.id}</strong>
                <span>{option.pipelineName}</span>
                <small>{shortDate(option.version.timestamp)}{option.version.status === "active" ? " - active" : ""}</small>
              </button>
              <label className="compare-check">
                <input
                  type="checkbox"
                  checked={compared}
                  onChange={() => onToggleComparison?.(option.version.id)}
                />
                <span>Compare</span>
              </label>
              <button className="version-pipeline-button" onClick={() => onSwitchPipelineVersion?.(option.version)}>
                {option.version.status === "active" ? "Open" : "Switch & open"}
              </button>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function VersionCompareModal({
  experiment,
  versions,
  onClose
}: {
  experiment: string;
  versions: FigureVersionOption[];
  onClose: () => void;
}) {
  return (
    <div className="preview-modal-backdrop" role="dialog" aria-modal="true" aria-label="Compare output versions">
      <section className="preview-modal compare-modal">
        <div className="preview-modal-header">
          <div>
            <h3>Compare versions</h3>
            <p>{versions.length} selected versions</p>
          </div>
          <button className="icon-action" onClick={onClose} title="Close comparison">
            <X size={16} />
          </button>
        </div>
        <div className="compare-modal-body">
          {versions.map((option) => (
            <VersionPreviewCard key={option.version.id} experiment={experiment} option={option} />
          ))}
        </div>
      </section>
    </div>
  );
}

function VersionPreviewCard({ experiment, option }: { experiment: string; option: FigureVersionOption }) {
  const [preview, setPreview] = useState<Preview | null>(null);
  const [previewError, setPreviewError] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setPreview(null);
    setPreviewError(false);
    api.preview(experiment, option.entry.id)
      .then((data) => {
        if (!cancelled) setPreview(data);
      })
      .catch(() => {
        if (!cancelled) setPreviewError(true);
      });
    return () => {
      cancelled = true;
    };
  }, [experiment, option.entry.id]);

  return (
    <section className="compare-card">
      <div className="compare-card-header">
        <div>
          <strong>#{option.version.id} {option.resultName}</strong>
          <span>{option.pipelineName}</span>
          <small>{shortDate(option.version.timestamp)}</small>
        </div>
        <a className="download-link" href={api.fileUrl(experiment, option.entry.id)} target="_blank" rel="noreferrer">
          Open
        </a>
      </div>
      <div className="compare-card-body">
        {preview?.kind === "table" && <CompactTablePreview preview={preview} />}
        {preview?.kind === "text" && <pre className="text-preview compact">{preview.text.slice(0, 3000)}</pre>}
        {preview?.kind === "file" && option.entry.extension === ".pdf" && (
          <PdfViewer url={api.fileUrl(experiment, option.entry.id)} title={option.resultName} />
        )}
        {preview?.kind === "file" && [".png", ".jpg", ".jpeg", ".svg"].includes(option.entry.extension) && (
          <img className="stage-image" src={api.fileUrl(experiment, option.entry.id)} alt={option.resultName} />
        )}
        {!preview && !previewError && <div className="friendly-empty"><strong>Loading preview...</strong><span>Fetching this version.</span></div>}
        {previewError && <div className="friendly-empty"><strong>Preview unavailable</strong><span>This version entry could not be loaded.</span></div>}
      </div>
    </section>
  );
}

function PreviewPanel({
  experiment,
  entry,
  preview,
  lineage,
  onOpenEntry,
  sourceFigureTarget,
  returnTargets = [],
  figureVersions = [],
  activeFigureVersionId,
  comparisonVersionIds = [],
  comparisonVersions = [],
  onPreviewFigureVersion,
  onToggleComparisonVersion,
  onOpenCompare,
  onOpenPipelineVersion,
  onSwitchPipelineVersion,
  embedded = false
}: {
  experiment: string;
  entry: Entry | null;
  preview: Preview | null;
  lineage: Lineage | null;
  onOpenEntry?: (entry: Entry, sourceFigure?: FigureReturnTarget | null) => void;
  sourceFigureTarget?: FigureReturnTarget | null;
  returnTargets?: FigureReturnTarget[];
  figureVersions?: FigureVersionOption[];
  activeFigureVersionId?: number | null;
  comparisonVersionIds?: number[];
  comparisonVersions?: FigureVersionOption[];
  onPreviewFigureVersion?: (option: FigureVersionOption) => void;
  onToggleComparisonVersion?: (versionId: number) => void;
  onOpenCompare?: () => void;
  onOpenPipelineVersion?: (version: Version) => void;
  onSwitchPipelineVersion?: (version: Version) => void;
  embedded?: boolean;
}) {
  const primaryReturnTarget = returnTargets[0];
  const content = (
    <>
      {entry && figureVersions.length > 0 && (
        <FigureVersionBrowser
          versions={figureVersions}
          activeVersionId={activeFigureVersionId}
          comparisonVersionIds={comparisonVersionIds}
          comparisonCount={comparisonVersions.length}
          onPreviewVersion={onPreviewFigureVersion}
          onToggleComparison={onToggleComparisonVersion}
          onOpenCompare={onOpenCompare}
          onOpenPipelineVersion={onOpenPipelineVersion}
          onSwitchPipelineVersion={onSwitchPipelineVersion}
        />
      )}
      {entry && preview?.kind === "table" && <TablePreview preview={preview} />}
      {entry && preview?.kind === "text" && <pre className="text-preview">{preview.text}</pre>}
      {entry && preview?.kind === "file" && entry.extension === ".pdf" && (
        <PdfViewer url={api.fileUrl(experiment, entry.id)} title={entry.fileName} />
      )}
      {entry && preview?.kind === "file" && [".png", ".jpg", ".jpeg", ".svg"].includes(entry.extension) && (
        <img className="image-preview" src={api.fileUrl(experiment, entry.id)} alt={entry.fileName} />
      )}
      {entry && (
        <details className="advanced-block" open={embedded}>
          <summary>Details and lineage</summary>
          <EntryMeta entry={entry} />
          {lineage && <LineagePanel lineage={lineage} onOpenEntry={onOpenEntry} activeEntryId={entry.id} sourceFigureTarget={sourceFigureTarget} />}
        </details>
      )}
      {!entry && <div className="empty-state">Choose a result or data entry to inspect its content and lineage.</div>}
    </>
  );
  if (embedded) {
    return <div className="preview-panel embedded">{content}</div>;
  }
  return (
    <section className="panel preview-panel">
      <div className="panel-header">
        <div>
          <h3>{entry?.fileName || "Preview"}</h3>
          <p>{entry ? `ID ${entry.id}  -  ${entry.extension || entry.type}` : "Select a file"}</p>
        </div>
        {entry && (
          <div className="preview-actions">
            {primaryReturnTarget && (
              <button
                className="return-link"
                title={`${primaryReturnTarget.resultName}  -  ${primaryReturnTarget.pipelineName}`}
                onClick={() => onOpenEntry?.(primaryReturnTarget.entry, null)}
              >
                <FileBarChart size={15} />
                Back to final output{returnTargets.length > 1 ? ` (${returnTargets.length})` : ""}
              </button>
            )}
            <a className="download-link" href={api.fileUrl(experiment, entry.id)} target="_blank" rel="noreferrer">
              <Download size={15} /> Open
            </a>
          </div>
        )}
      </div>
      {content}
    </section>
  );
}

function EntryMeta({ entry }: { entry: Entry }) {
  return (
    <div className="entry-meta">
      <KeyValue label="Path" value={entry.path} />
      <KeyValue label="Original" value={entry.originalPath || "-"} />
      <KeyValue label="Parents" value={entry.parentIds.length ? entry.parentIds.join(", ") : "none"} />
      <div className="tag-row">
        {entry.tags.map((tag) => <span key={tag}>{tag}</span>)}
      </div>
    </div>
  );
}

function LineagePanel({
  lineage,
  onOpenEntry,
  activeEntryId,
  sourceFigureTarget
}: {
  lineage: Lineage;
  onOpenEntry?: (entry: Entry, sourceFigure?: FigureReturnTarget | null) => void;
  activeEntryId?: number;
  sourceFigureTarget?: FigureReturnTarget | null;
}) {
  const sorted = [...lineage.nodes].sort((a, b) => a.id - b.id);
  return (
    <div className="lineage-panel">
      <div className="lineage-heading">
        <Network size={15} />
        <strong>Lineage</strong>
        <span>{lineage.nodes.length} nodes  -  {lineage.edges.length} edges</span>
      </div>
      <div className="lineage-list">
        {sorted.map((node) => (
          <button
            key={node.id}
            type="button"
            className={`lineage-node ${activeEntryId === node.id ? "active" : ""}`}
            onClick={() => onOpenEntry?.(node, sourceFigureTarget)}
          >
            <span className={`type-badge ${node.type}`}>{node.type}</span>
            <strong>ID {node.id}</strong>
            <span>{node.processor}</span>
            <small>{node.fileName || node.originalPath || node.path}</small>
          </button>
        ))}
      </div>
    </div>
  );
}

function TablePreview({ preview }: { preview: Extract<Preview, { kind: "table" }> }) {
  return (
    <div className="table-preview">
      <p>{preview.shape[0]} rows x {preview.shape[1]} columns</p>
      <table>
        <thead>
          <tr>{preview.columns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {preview.rows.map((row, index) => (
            <tr key={index}>
              {preview.columns.map((column) => <td key={column}>{String(row[column] ?? "")}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ProcessorStudio({ experiment, onSaved }: { experiment: string; onSaved: () => Promise<void> }) {
  const [templates, setTemplates] = useState<ProcessorTemplate[]>([]);
  const [templateId, setTemplateId] = useState("");
  const [filename, setFilename] = useState("custom_processor.py");
  const [code, setCode] = useState("");
  const [validation, setValidation] = useState<ProcessorValidation | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");

  useEffect(() => {
    let cancelled = false;
    api.processorTemplates().then((items) => {
      if (cancelled) return;
      setTemplates(items);
      if (items.length && !code) {
        setTemplateId(items[0].id);
        setFilename(items[0].filename);
        setCode(items[0].code);
      }
    }).catch((err) => setMessage(err instanceof Error ? err.message : "Template load failed"));
    return () => { cancelled = true; };
  }, []);

  const selectedTemplate = templates.find((item) => item.id === templateId);

  function useTemplate(nextId: string) {
    const next = templates.find((item) => item.id === nextId);
    setTemplateId(nextId);
    if (next) {
      setFilename(next.filename);
      setCode(next.code);
      setValidation(null);
      setMessage("");
    }
  }

  async function validateDraft() {
    if (!experiment) return null;
    setBusy(true);
    setMessage("");
    try {
      const result = await api.validateProcessor(experiment, filename, code);
      setValidation(result);
      return result;
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Validation failed");
      return null;
    } finally {
      setBusy(false);
    }
  }

  async function saveDraft() {
    const result = await validateDraft();
    if (!result || !result.ok) return;
    setBusy(true);
    setMessage("");
    try {
      const saved = await api.saveProcessor(experiment, filename, code);
      setFilename(saved.filename);
      setValidation(saved.validation);
      setMessage(`Saved ${saved.filename}`);
      await onSaved();
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Save failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="panel processor-studio-panel">
      <div className="panel-header">
        <div>
          <h3>Processor Studio</h3>
          <p>Advanced tool for custom parsing and data transforms. Start from a template, validate the FileLine format, then save it for this experiment.</p>
        </div>
        <FileCode size={18} />
      </div>
      <div className="processor-studio-grid">
        <label className="processor-studio-field">
          <span>Template</span>
          <select value={templateId} onChange={(event) => useTemplate(event.target.value)}>
            {templates.map((template) => (
              <option key={template.id} value={template.id}>{template.name}</option>
            ))}
          </select>
        </label>
        <label className="processor-studio-field">
          <span>Filename</span>
          <input value={filename} onChange={(event) => setFilename(event.target.value)} placeholder="custom_processor.py" />
        </label>
      </div>
      <textarea
        className="processor-code-editor"
        spellCheck={false}
        value={code}
        onChange={(event) => {
          setCode(event.target.value);
          setValidation(null);
          setMessage("");
        }}
      />
      <div className="processor-studio-actions">
        <button className="secondary-save-button" disabled={busy || !code.trim()} onClick={validateDraft}>
          <CheckCircle size={15} /> {busy ? "Checking" : "Validate"}
        </button>
        <button className="primary-save-button" disabled={busy || !code.trim()} onClick={saveDraft}>
          <FileCode size={15} /> {busy ? "Saving" : "Save processor"}
        </button>
        {message && <span className="processor-studio-message">{message}</span>}
      </div>
      {validation && (
        <div className={`processor-validation ${validation.ok ? "ok" : "failed"}`}>
          <strong>{validation.ok ? "Format ready" : "Fix required"}</strong>
          {validation.processors.length > 0 && (
            <div className="processor-validation-list">
              {validation.processors.map((processor) => (
                <div key={`${processor.name}-${processor.function}`} className="processor-validation-card">
                  <span>{processor.name}</span>
                  <small>{processor.inputType} input - {processor.outputType} output - {processor.outputExt}</small>
                  {processor.params.length > 0 && <em>params: {processor.params.join(", ")}</em>}
                </div>
              ))}
            </div>
          )}
          {validation.errors.length > 0 && (
            <ul>
              {validation.errors.map((item) => <li key={item}>{item}</li>)}
            </ul>
          )}
          {validation.warnings.length > 0 && (
            <ul className="processor-warning-list">
              {validation.warnings.map((item) => <li key={item}>{item}</li>)}
            </ul>
          )}
        </div>
      )}
    </section>
  );
}

function ProcessorsView({
  experiment,
  selectedPipelinePath,
  pipelines,
  setSelectedPipelinePath,
  onRefresh
}: {
  experiment: string;
  selectedPipelinePath: string;
  pipelines: PipelineSummary[];
  setSelectedPipelinePath: (path: string) => void;
  onRefresh: () => Promise<void>;
}) {
  const [storage, setStorage] = useState<StorageReport | null>(null);
  const [storageBusy, setStorageBusy] = useState(false);
  const [deleteBusyId, setDeleteBusyId] = useState<number | null>(null);
  const [storageMessage, setStorageMessage] = useState("");

  const refreshStorage = useCallback(async () => {
    if (!experiment) return;
    setStorageBusy(true);
    setStorageMessage("");
    try {
      const [experimentStorage, pipelineStorage] = await Promise.all([
        api.storage(experiment),
        api.storage(experiment, selectedPipelinePath || undefined)
      ]);
      setStorage({
        ...experimentStorage,
        pipeline: pipelineStorage.pipeline,
        versions: pipelineStorage.versions
      });
    } catch (err) {
      setStorageMessage(err instanceof Error ? err.message : "Storage report failed");
    } finally {
      setStorageBusy(false);
    }
  }, [experiment, selectedPipelinePath]);

  useEffect(() => {
    refreshStorage();
  }, [refreshStorage]);

  async function deleteVersion(version: StorageReport["versions"][number]) {
    if (!experiment || !version.deletable) return;
    const label = `#${version.id} ${version.exportName || version.configFile || "version"}`;
    const ok = window.confirm(`Delete ${label} - \n\nEstimated reclaimable space: ${formatBytes(version.reclaimableBytes)}.\nActive versions and shared files will be kept.`);
    if (!ok) return;
    setDeleteBusyId(version.id);
    setStorageMessage("");
    try {
      const result = await api.deleteVersion(experiment, version.id, selectedPipelinePath || undefined);
      setStorageMessage(`Deleted version #${version.id}. Freed ${formatBytes(result.freedBytes)} across ${result.deletedEntries} entries.`);
      await Promise.all([refreshStorage(), onRefresh()]);
    } catch (err) {
      setStorageMessage(err instanceof Error ? err.message : "Delete failed");
    } finally {
      setDeleteBusyId(null);
    }
  }

  const visiblePipelines = currentExperimentPipelines(pipelines, experiment);
  const selectedPipeline = visiblePipelines.find((item) => item.path === selectedPipelinePath);
  const categories = storage?.byCategory || {};
  const storageRows = [
    ["Raw source data", categories.raw || 0],
    ["Processed and plots", categories.processed || 0],
    ["Synced exports", categories.exports || 0],
    ["Processor snapshots", categories.processorSnapshots || 0],
    ["Experiment processors", categories.processors || 0],
    ["Pipeline snapshots", categories.pipelines || 0],
    ["Database", storage?.databaseBytes || 0],
    ["Other", categories.other || 0]
  ];

  return (
    <div className="advanced-grid">
      <section className="panel advanced-control-panel">
        <div className="panel-header">
          <div>
            <h3>Pipeline</h3>
            <p>{selectedPipeline?.description || "Choose the pipeline whose versions you want to manage"}</p>
          </div>
          <GitBranch size={18} />
        </div>
        <div className="advanced-pipeline-control">
          <label className="field-label">Current pipeline</label>
          <select value={selectedPipelinePath} onChange={(event) => setSelectedPipelinePath(event.target.value)}>
            {visiblePipelines.map((item) => (
              <option key={item.path} value={item.path}>{item.group} / {item.name}</option>
            ))}
          </select>
          {selectedPipeline && (
            <div className="advanced-pipeline-meta">
              <span>{selectedPipeline.stepCount} steps</span>
              <span>{selectedPipeline.exportCount} exports</span>
              <span>{selectedPipeline.sourceCount} sources</span>
            </div>
          )}
        </div>
      </section>

      <section className="panel storage-panel">
        <div className="panel-header">
          <div>
            <h3>Storage</h3>
            <p>{storage ? `${formatBytes(storage.totalBytes)} used by the whole experiment` : "Measure experiment disk usage"}</p>
          </div>
          <button className="icon-action" onClick={refreshStorage} disabled={storageBusy} title="Refresh storage">
            <RefreshCw size={16} />
          </button>
        </div>
        <div className="storage-summary">
          <div className="storage-total">
            <HardDrive size={22} />
            <div>
              <span>Total usage</span>
              <strong>{storage ? formatBytes(storage.totalBytes) : storageBusy ? "Measuring" : "--"}</strong>
            </div>
          </div>
          <div className="pipeline-storage-summary">
            <span>Selected pipeline</span>
            <strong>{selectedPipeline?.name || storage?.pipeline.configFile || "Pipeline"}</strong>
            <small>
              {storage
                ? `${storage.pipeline.versionCount} versions  -  ${formatBytes(storage.pipeline.reclaimableBytes)} reclaimable from this pipeline`
                : "Version cleanup is filtered by the selected pipeline"}
            </small>
          </div>
          <div className="storage-breakdown">
            {storageRows.map(([label, value]) => (
              <div key={label} className="storage-row">
                <span>{label}</span>
                <strong>{formatBytes(Number(value))}</strong>
              </div>
            ))}
          </div>
          {storage?.basePath && <small className="storage-path">{storage.basePath}</small>}
          {storageMessage && <div className="storage-message">{storageMessage}</div>}
        </div>
      </section>

      <section className="panel storage-panel">
        <div className="panel-header">
          <div>
            <h3>Version cleanup</h3>
            <p>{selectedPipeline ? `Filtered to ${selectedPipeline.name}` : "Filtered by the selected pipeline"}</p>
          </div>
          <Trash2 size={18} />
        </div>
        <div className="cleanup-list">
          {storage?.versions.length ? storage.versions.map((version) => (
            <div key={version.id} className={`cleanup-row ${version.status === "active" ? "active" : ""}`}>
              <div>
                <strong>#{version.id} {version.exportName || version.configFile || "pipeline"}</strong>
                <span>{shortDate(version.timestamp)}  -  {version.entryCount} entries  -  {version.exclusiveProcessedEntries} exclusive</span>
              </div>
              <div className="cleanup-actions">
                <b>{formatBytes(version.reclaimableBytes)}</b>
                {version.deletable ? (
                  <button
                    className="danger-button"
                    disabled={deleteBusyId === version.id}
                    onClick={() => deleteVersion(version)}
                  >
                    {deleteBusyId === version.id ? "Deleting" : "Delete"}
                  </button>
                ) : (
                  <small>Active</small>
                )}
              </div>
            </div>
          )) : (
            <div className="friendly-empty">
              <strong>No version records yet.</strong>
              <span>Run a pipeline to create recoverable versions.</span>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}

function PdfViewer({ url, title }: { url: string; title: string }) {
  return (
    <div className="pdf-viewer-wrap">
      <div className="pdf-toolbar">
        <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", minWidth: 0 }}>{title}</span>
        <a className="download-link" href={url} target="_blank" rel="noreferrer">Open</a>
      </div>
      <iframe
        className="pdf-frame"
        title={title || "PDF preview"}
        src={`${url}#view=FitH`}
      />
    </div>
  );
}

function KeyValue({ label, value }: { label: string; value: string }) {
  return (
    <div className="key-value">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function JsonBlock({ title, value }: { title: string; value: unknown }) {
  return (
    <details open>
      <summary>{title}</summary>
      <pre className="code-block">{JSON.stringify(value, null, 2)}</pre>
    </details>
  );
}
