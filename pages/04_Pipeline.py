"""FileLine - 流水线管理"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
from sqlalchemy.orm import selectinload
from app_utils import get_processor_list
from core.base import get_session
from core.models import DataEntry, StepCache

st.set_page_config(page_title="流水线", page_icon="🔗", layout="wide")

st.title("🔗 流水线管理")
st.caption("查看流水线执行历史和处理器注册信息")

processors = get_processor_list()

tab1, tab2, tab3 = st.tabs(["📋 执行历史", "🧩 注册的处理器", "📄 YAML 模板"])

# ========== Tab 1: 执行历史 ==========
with tab1:
    st.subheader("流水线执行历史")
    with get_session() as session:
        caches = (
            session.query(StepCache)
            .order_by(StepCache.created_at.desc())
            .limit(50)
            .all()
        )

    if not caches:
        st.info("暂无流水线执行记录。请通过 CLI 运行流水线: `python main.py pipeline run <config>`")
    else:
        rows = []
        for c in caches:
            rows.append(
                {
                    "ID": c.id,
                    "Hash": c.input_hash[:12] + "...",
                    "输出 ID": c.output_id,
                    "时间": c.created_at.strftime("%m-%d %H:%M:%S"),
                }
            )
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    # 数据血缘关系可视化
    st.divider()
    st.subheader("🔗 数据血缘关系")

    # 用安全的 entry 列表
    from app_utils import get_data_entries

    entries = get_data_entries(limit=30)
    if entries:
        selected_id = st.selectbox(
            "选择一个数据条目查看其血缘关系",
            options=[e.id for e in entries],
            format_func=lambda x: f"ID {x} - {next((e for e in entries if e.id == x), Path('').name)}",
        )

        if selected_id:
            with get_session() as session:
                entry = (
                    session.query(DataEntry)
                    .options(
                        selectinload(DataEntry.tags),
                        selectinload(DataEntry.parents),
                    )
                    .filter(DataEntry.id == selected_id)
                    .first()
                )
                if entry:
                    st.markdown(f"**当前:** ID {entry.id} `{Path(entry.path).name}` ({entry.type})")
                    st.markdown(f"**标签:** {', '.join(t.name for t in entry.tags) or '无'}")

                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**⬆️ 祖先 (输入):**")
                        if entry.parents:
                            for p in entry.parents:
                                st.markdown(f"- ID {p.id}: `{Path(p.path).name}` ({p.type})")
                        else:
                            st.caption("无祖先 (原始数据)")

                    with col2:
                        st.markdown("**⬇️ 后代 (输出):**")
                        descendants = (
                            session.query(DataEntry)
                            .options(selectinload(DataEntry.tags))
                            .filter(DataEntry.parents.any(id=selected_id))
                            .all()
                        )
                        if descendants:
                            for d in descendants:
                                st.markdown(f"- ID {d.id}: `{Path(d.path).name}` ({d.type})")
                        else:
                            st.caption("无后代")

# ========== Tab 2: 处理器列表 ==========
with tab2:
    st.subheader("注册的处理器")

    for group_name, group_procs in processors.items():
        if not group_procs:
            continue
        with st.expander(
            f"{'📈' if group_name == 'plot' else '🛠️'} {group_name.upper()} ({len(group_procs)})",
            expanded=group_name == "plot",
        ):
            for name, info in group_procs.items():
                st.markdown(f"**`{name}`**")
                st.caption(
                    f"输入类型: `{info['input_type']}` | 输出后缀: `{info['output_ext']}` | 版本: `{info['hash'][:8]}`"
                )
                st.markdown("---")

# ========== Tab 3: YAML 模板 ==========
with tab3:
    st.subheader("YAML 流水线模板")

    template_type = st.selectbox(
        "选择模板类型",
        [
            "折线图 (line)",
            "柱状图 (bar)",
            "分组柱状图 (grouped_bar)",
            "水平柱状图 (hbar)",
            "双Y轴折线图 (dual_axis)",
            "类别折线图 (line_category)",
            "时间线图 (timeline_hbar)",
        ],
    )

    templates = {
        "折线图 (line)": """# 折线图流水线配置
initial_load:
  include_patterns:
    - path: "path/to/*.csv"

steps:
  - processor: plot_line
    inputs: initial
    output_var: plot_result
    export: my_line_plot
    params:
      time_col: "time"
      value_col: "loss"
      tag_col: "method"
      title: "训练曲线"
      xlabel: "时间 (s)"
      ylabel: "Loss"
""",
        "柱状图 (bar)": """# 柱状图流水线配置
initial_load:
  include_patterns:
    - path: "path/to/*.csv"

steps:
  - processor: plot_bar
    inputs: initial
    output_var: plot_result
    export: my_bar_chart
    params:
      x_col: "category"
      value_col: "value"
      title: "柱状图"
      xlabel: "类别"
      ylabel: "数值"
""",
        "分组柱状图 (grouped_bar)": """# 分组柱状图流水线配置
initial_load:
  include_patterns:
    - path: "path/to/*.csv"

steps:
  - processor: plot_grouped_bar
    inputs: initial
    output_var: plot_result
    export: my_grouped_bar
    params:
      main_group_col: "model"
      sub_group_col: "dataset"
      value_col: "accuracy"
      title: "模型精度对比"
      xlabel: "模型"
      ylabel: "Accuracy (%)"
""",
        "水平柱状图 (hbar)": """# 水平柱状图流水线配置
initial_load:
  include_patterns:
    - path: "path/to/*.csv"

steps:
  - processor: plot_horizontal_bar
    inputs: initial
    output_var: plot_result
    export: my_hbar
    params:
      y_col: "method"
      value_col: "latency"
      title: "延迟对比"
      xlabel: "Latency (ms)"
      ylabel: "方法"
""",
        "双Y轴折线图 (dual_axis)": """# 双Y轴折线图流水线配置
initial_load:
  include_patterns:
    - path: "path/to/*.csv"

steps:
  - processor: plot_dual_axis_line
    inputs: initial
    output_var: plot_result
    export: my_dual_axis
    params:
      time_col: "step"
      value_cols_y1: ["loss"]
      value_cols_y2: ["accuracy"]
      ylabel_y1: "Loss"
      ylabel_y2: "Accuracy"
      title: "训练过程"
""",
        "类别折线图 (line_category)": """# 类别折线图流水线配置
initial_load:
  include_patterns:
    - path: "path/to/*.csv"

steps:
  - processor: plot_line_categorical
    inputs: initial
    output_var: plot_result
    export: my_cat_line
    params:
      x_col: "category"
      value_col: "score"
      tag_col: "method"
      title: "类别对比"
      xlabel: "类别"
      ylabel: "Score"
""",
        "时间线图 (timeline_hbar)": """# 时间线水平条图流水线配置
initial_load:
  include_patterns:
    - path: "path/to/*.csv"

steps:
  - processor: plot_timeline_hbar
    inputs: initial
    output_var: plot_result
    export: my_timeline
    params:
      category_col: "task"
      sub_category_col: "subtask"
      start_col: "start_time"
      end_col: "end_time"
      title: "时间线"
      xlabel: "Time"
""",
    }

    st.code(templates[template_type], language="yaml")
    st.info(
        "💡 将以上配置保存为 `.yaml` 文件，然后通过 CLI 运行: `python main.py pipeline run config.yaml`"
    )
