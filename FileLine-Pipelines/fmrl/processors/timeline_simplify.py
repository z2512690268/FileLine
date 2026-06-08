"""Timeline CSV 简化: 仅做 remap, 不做任何合并/吸收"""
import pandas as pd
from core.processing import ProcessorRegistry


@ProcessorRegistry.register("simplify_timeline_kinds", input_type="single", output_ext=".csv")
def simplify_timeline_kinds(input_path, output_path):
    """将 timeline CSV 的 kind_label 重新映射为论文标签.

    - GPU 0/1: 保留 Rollout / TrainPrep / Reward+Train / Wait for Tail / Cluster Rebuild / Init / Idle
    - GPU 2: 保留 Tail Continuation / Idle (与 plot_timeline.py 完全一致)
    """
    df = pd.read_csv(input_path.path)

    remap = {
        "Init / Warmup":      "Init",
        "Generation (vLLM)":  "Rollout",
        "Reward Compute":     "Reward+Train",
        "Training (Megatron)":"Reward+Train",
        "Param Update":       "TrainPrep",
    }
    df["kind_label"] = df["kind_label"].map(remap).fillna(df["kind_label"])
    df = df.sort_values(["display_group", "start_sec"])

    # GPU 2 idle 是状态机内部标记 (tail worker 等下一批), 不在论文图中渲染
    df = df[~((df["gpu_id"] == "GPU 2") & (df["kind_label"] == "Idle"))]

    # 仅合并相邻同类段 (首尾相连)
    merged = []
    for (grp, kind), group in df.groupby(["display_group", "kind_label"], sort=False):
        group = group.sort_values("start_sec")
        rows = group.to_dict("records")
        cur = rows[0]
        out = [cur]
        for r in rows[1:]:
            last = out[-1]
            if r["start_sec"] <= last["end_sec"] + 0.1:
                last["end_sec"] = max(last["end_sec"], r["end_sec"])
                last["duration"] = last["end_sec"] - last["start_sec"]
            else:
                out.append(r)
        merged.extend(out)
    df = pd.DataFrame(merged)
    df.to_csv(output_path, index=False)
    return [f"simplified:{len(df)}segs"]
