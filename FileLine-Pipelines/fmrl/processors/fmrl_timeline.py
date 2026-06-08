"""FMRL 日志 → GPU Timeline 数据: 直接复用 FMRL/scripts/plot_timeline.py 的逻辑"""
import pandas as pd
from pathlib import Path
from core.processing import ProcessorRegistry

# 直接从原版 plot_timeline.py 导入
import sys as _sys
_fmrl_scripts = "/home/keyao/cloudcli/FMRL/scripts"
if _fmrl_scripts not in _sys.path:
    _sys.path.insert(0, _fmrl_scripts)
import plot_timeline as _pt

# 重新导出, 保持接口一致
parse_log = _pt.parse_log
build_segments = _pt.build_segments
normalize_task = _pt.normalize_task

KIND_LABELS = {
    'generation': 'Generation (vLLM)', 'tail_gen': 'Tail Continuation',
    'train': 'Training (Megatron)', 'reward': 'Reward Compute',
    'model_update': 'Param Update', 'rebuild': 'Cluster Rebuild',
    'wait_tail': 'Wait for Tail', 'init': 'Init / Warmup', 'idle': 'Idle',
}

KIND_CATEGORY = {
    'generation': 'infer', 'tail_gen': 'infer',
    'train': 'train', 'reward': 'train',
    'model_update': 'train', 'rebuild': 'train',
    'wait_tail': 'infer', 'init': 'infer',
    'idle': 'idle',
}


@ProcessorRegistry.register("parse_fmrl_timeline", input_type="single", output_ext=".csv")
def parse_fmrl_timeline(input_path, output_path, offset_by_first_event=True):
    """解析 FMRL subprocess log, 输出 GPU Timeline DataFrame

    直接使用 FMRL/scripts/plot_timeline.py 的 parse_log + build_segments,
    保证数据与 plot_timeline.py 完全一致.
    """
    events, main_gpus, tail_gpus, t0, t1 = _pt.parse_log(str(input_path.path))
    if not events:
        return []

    segments = _pt.build_segments(events, main_gpus, tail_gpus)
    ref_time = events[0][0] if offset_by_first_event else t0
    rows = []
    for gpu in sorted(segments):
        by_task: dict = {}
        for s, e, kind, task in segments[gpu]:
            task_name = _pt.normalize_task(task) or ''
            by_task.setdefault(task_name, []).append((s, e, kind, task))

        def _task_key(t):
            if not t:
                return (1, 999)
            parts = t.split('_')
            return (0, int(parts[-1]) if parts[-1].isdigit() else 0)
        task_order = sorted(by_task.keys(), key=_task_key)
        for task_name in task_order:
            if not task_name:
                continue
            segs = by_task[task_name]
            display = f"GPU {gpu} [{_pt.short_task(task_name):>3s}]"
            for s, e, kind, task in segs:
                rows.append({
                    'display_group': display,
                    'gpu_id': f'GPU {gpu}',
                    'task': task_name,
                    'kind': kind,
                    'kind_label': KIND_LABELS.get(kind, kind),
                    'category': KIND_CATEGORY.get(kind, 'idle'),
                    'start_sec': (s - ref_time).total_seconds(),
                    'end_sec': (e - ref_time).total_seconds(),
                    'duration': (e - s).total_seconds(),
                })

    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)

    summary = df.groupby(['gpu_id', 'task', 'kind', 'kind_label'])['duration'].sum().reset_index()
    summary.to_csv(Path(output_path).parent / f"{Path(output_path).stem}_summary.csv", index=False)

    total = (t1 - t0).total_seconds()
    return [f"timeline:{len(df)}segs", f"total:{int(total)}s"]
