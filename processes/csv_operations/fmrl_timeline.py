"""FMRL 日志 → GPU Timeline 数据: 解析事件, 构建时间线 DataFrame"""
import re
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional, Tuple, Set
import pandas as pd
from core.processing import ProcessorRegistry

# ── 日志解析 ──────────────────────────────────────────────
RE_TS = re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]')
TASK_PATTERN = r'fmrl_subprocess_debug(?:_\d+)?'


def normalize_task(name):
    if not name:
        return None
    m = re.search(TASK_PATTERN, name)
    return m[0] if m else None


def extract_task(line):
    patterns = [
        rf'\b(?:task|rl_task_id)=({TASK_PATTERN})(?=[\s,:)\]]|$)',
        rf'\btask_id=({TASK_PATTERN})(?=[\s,:)\]]|$)',
        rf'\b({TASK_PATTERN})__actor_[\w-]+',
    ]
    for pat in patterns:
        m = re.search(pat, line)
        if m:
            return normalize_task(m[1])
    return None


def parse_time(s):
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')


@dataclass
class Event:
    ts: datetime
    kind: str
    task: Optional[str]
    detail: str


def append_event(events, ts, kind, task, detail):
    events.append(Event(ts, kind, normalize_task(task) or task, detail))


def parse_log(path: str) -> Tuple[List[Event], List[int], List[int], datetime, datetime]:
    lines = Path(path).read_text().splitlines()
    events = []
    cur_task = None
    last_ts = None
    main_gpus, tail_gpus = [0, 1], [2]
    active_tail_tasks: Set[str] = set()
    seen_accumulates = set()

    for line in lines:
        m = RE_TS.search(line)
        ts = parse_time(m[1]) if m else last_ts
        if ts is None:
            continue
        last_ts = ts

        lt = extract_task(line)
        if lt:
            cur_task = lt

        # GPU mappings
        if 'Split actor_infer device_mapping:' in line:
            m = re.search(r'main_pool=\[([^\]]*)\].*?tail_pool=\[([^\]]*)\]', line)
            if m:
                main_gpus = [int(x.strip()) for x in m[1].split(',') if x.strip()]
                tail_gpus = [int(x.strip()) for x in m[2].split(',') if x.strip()]

        # Phase transitions
        if re.search(r'phase:\s*None\s*->\s*rollout_ready', line):
            append_event(events, ts, 'ready', cur_task, 'Ready')
            continue

        m = re.search(r'phase:\s*(\S+)\s*->\s*(\S+)', line)
        if m:
            from_phase, to = m[1], m[2]
            if to == 'rollout_running':
                append_event(events, ts, 'gen_start', cur_task, 'Rollout')
            elif from_phase == 'rollout_running' and to in ('rollout_ready', 'longtail_waiting', 'accumulating', 'reward_train_ready'):
                append_event(events, ts, 'gen_done', cur_task, 'Generated')
            if to in ('longtail_waiting', 'accumulating'):
                append_event(events, ts, 'wait_tail', cur_task, 'Wait')
            elif to == 'reward_train_ready':
                append_event(events, ts, 'tail_merge', cur_task, 'Merge')
            elif to == 'reward_running':
                append_event(events, ts, 'reward_start', cur_task, 'Reward')
            elif to == 'train_running':
                append_event(events, ts, 'train_start', cur_task, 'Train')
            if from_phase == 'train_running' and to in ('rollout_ready', 'done'):
                append_event(events, ts, 'train_done', cur_task, 'TrainDone')
            continue

        # Main-pool specific
        if 'main_pool generated batch' in line:
            append_event(events, ts, 'gen_done', cur_task, 'Generated')

        if ('main_pool sent ACCUMULATE' in line
                or '[AccumulateMessage] sent ACCUMULATE' in line
                or '[AccumulateMessage] sent deferred ACCUMULATE' in line):
            key = (ts, cur_task)
            if key not in seen_accumulates:
                seen_accumulates.add(key)
                append_event(events, ts, 'tail_submit', cur_task, 'TailSubmit')
            if cur_task not in active_tail_tasks:
                active_tail_tasks.add(cur_task)
                append_event(events, ts, 'tail_start', cur_task, 'TailStart')

        if 'merged tail subprocess result into main flow' in line:
            append_event(events, ts, 'tail_merge', cur_task, 'TailMerge')

        if 'finalized accumulation' in line:
            was = cur_task in active_tail_tasks
            active_tail_tasks.discard(cur_task)
            if was:
                append_event(events, ts, 'tail_done', cur_task, 'TailDone')

        if 'main_pool applied model update before generation' in line:
            append_event(events, ts, 'model_update', cur_task, 'ModelUpdate')

        if 'main_pool idle wait' in line:
            append_event(events, ts, 'idle_start', cur_task, 'Idle')

        if 'Rebuilding rollout cluster' in line:
            append_event(events, ts, 'rebuild', cur_task, 'Rebuild')

        if 'Initializing a V1 LLM engine' in line:
            append_event(events, ts, 'init', cur_task, 'vLLMInit')

        if 'all per-task iteration targets reached' in line:
            append_event(events, ts, 'done', cur_task, 'Done')

    events.sort(key=lambda e: e.ts)
    t0 = events[0].ts if events else datetime.now()
    t1 = events[-1].ts if events else datetime.now()
    return events, main_gpus, tail_gpus, t0, t1


def build_segments(events, main_gpus, tail_gpus, t0, t1):
    """Return list of (start, end, kind, task) per GPU"""
    seg_main, seg_tail = [], []
    state0, t0_start, task0 = 'idle', t0, None
    state1, t1_start, task1 = 'idle', t0, None

    def flush(segs, state, ts, new_state, s_start, task, new_task=None):
        dur = (ts - s_start).total_seconds()
        if dur > 1 and state not in (None, 'idle') or (state == 'idle' and dur >= 5):
            segs.append((s_start, ts, state, task))
        return new_state, ts, new_task if new_task is not None else task

    for e in events:
        # GPU0 (main)
        if e.kind == 'gen_start':
            state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'generation', t0_start, task0, e.task)
        elif e.kind == 'gen_done':
            state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'idle', t0_start, task0)
        elif e.kind == 'wait_tail' and state0 != 'wait_tail':
            state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'wait_tail', t0_start, task0)
        elif e.kind == 'reward_start':
            state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'reward', t0_start, task0)
        elif e.kind == 'train_start':
            state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'train', t0_start, task0)
        elif e.kind == 'train_done':
            state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'idle', t0_start, task0)
        elif e.kind == 'model_update':
            state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'model_update', t0_start, task0)
        elif e.kind == 'rebuild':
            state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'rebuild', t0_start, task0)
        elif e.kind == 'init':
            state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'init', t0_start, task0, e.task)
        elif e.kind == 'done':
            if state0 not in ('idle', None):
                state0, t0_start, task0 = flush(seg_main, state0, e.ts, 'idle', t0_start, task0)

        # GPU1 (tail)
        if e.kind == 'tail_start':
            state1, t1_start, task1 = flush(seg_tail, state1, e.ts, 'tail_gen', t1_start, task1, e.task)
        elif e.kind == 'tail_done':
            state1, t1_start, task1 = flush(seg_tail, state1, e.ts, 'idle', t1_start, task1)

    end = events[-1].ts if events else t1
    if state0 not in ('idle', None):
        flush(seg_main, state0, end, None, t0_start, task0)
    if state1 not in ('idle', None):
        flush(seg_tail, state1, end, None, t1_start, task1)

    segments = {gpu: [] for gpu in sorted(set(main_gpus + tail_gpus))}
    for gpu in main_gpus:
        segments[gpu] = list(seg_main)
    for gpu in tail_gpus:
        segments[gpu] = list(seg_tail)
    return segments


KIND_LABELS = {
    'generation': 'Generation (vLLM)', 'tail_gen': 'Tail Continuation',
    'train': 'Training (Megatron)', 'reward': 'Reward Compute',
    'model_update': 'Param Update', 'rebuild': 'Cluster Rebuild',
    'wait_tail': 'Wait for Tail', 'init': 'Init / Warmup', 'idle': 'Idle',
}


@ProcessorRegistry.register("parse_fmrl_timeline", input_type="single", output_ext=".csv")
def parse_fmrl_timeline(input_path, output_path, offset_by_first_event=True):
    """解析 FMRL subprocess log, 输出 GPU Timeline DataFrame"""
    events, main_gpus, tail_gpus, t0, t1 = parse_log(str(input_path.path))
    if not events:
        return []

    segments = build_segments(events, main_gpus, tail_gpus, t0, t1)
    ref_time = events[0].ts if offset_by_first_event else t0
    rows = []
    for gpu in sorted(segments):
        for s, e, kind, task in segments[gpu]:
            task_name = normalize_task(task) or ''
            rows.append({
                'gpu_id': f'GPU {gpu}',
                'task': task_name,
                'kind': kind,
                'kind_label': KIND_LABELS.get(kind, kind),
                'start_sec': (s - ref_time).total_seconds(),
                'end_sec': (e - ref_time).total_seconds(),
                'duration': (e - s).total_seconds(),
            })

    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)

    # 按 task 汇总
    summary = df.groupby(['gpu_id', 'task', 'kind', 'kind_label'])['duration'].sum().reset_index()
    summary.to_csv(Path(output_path).parent / f"{Path(output_path).stem}_summary.csv", index=False)

    total = t1 - t0
    return [f"timeline:{len(df)}segs", f"total:{int(total.total_seconds())}s"]
