"""FileLine 可视化数据处理平台 - Streamlit 入口"""

import sys
import os
from pathlib import Path

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import streamlit as st
from sqlalchemy.orm import selectinload
from core.base import experiment_manager, init_db, get_session
from core.models import DataEntry

st.set_page_config(
    page_title="FileLine 数据平台",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------- 初始化 Session State ----------
if "current_experiment" not in st.session_state:
    st.session_state.current_experiment = experiment_manager.current_experiment
if "data_entries" not in st.session_state:
    st.session_state.data_entries = []


def load_experiments():
    """加载并返回所有实验列表"""
    return experiment_manager.get_experiments()


def switch_experiment(name):
    """切换当前实验"""
    experiment_manager.set_current(name, persist=True)
    st.session_state.current_experiment = name
    st.rerun()


def get_experiment_stats():
    """获取当前实验的统计数据"""
    try:
        with get_session() as session:
            total = session.query(DataEntry).count()
            raw = session.query(DataEntry).filter(DataEntry.type == "raw").count()
            processed = (
                session.query(DataEntry).filter(DataEntry.type == "processed").count()
            )
            from sqlalchemy import or_
            plot_extensions = ("%.pdf", "%.png", "%.jpg", "%.jpeg", "%.svg")
            plot_count = (
                session.query(DataEntry)
                .filter(DataEntry.type == "processed")
                .filter(or_(*(DataEntry.path.like(pat) for pat in plot_extensions)))
                .count()
            )
            return {"total": total, "raw": raw, "processed": processed, "plot": plot_count}
    except Exception:
        return {"total": 0, "raw": 0, "processed": 0, "plot": 0}


# ---------- 侧边栏 ----------
with st.sidebar:
    st.title("📊 FileLine")
    st.caption("科研数据流自动化管理 & 可视化平台")

    st.divider()

    # 实验选择
    st.subheader("🔬 实验管理")
    experiments = load_experiments()

    if not experiments:
        st.warning("暂无实验，请先通过 CLI 创建: `python main.py experiment create <名称>`")
    else:
        exp_names = list(experiments.keys())
        current = st.session_state.current_experiment

        if current and current in exp_names:
            idx = exp_names.index(current)
        else:
            idx = 0

        selected = st.selectbox(
            "选择实验",
            exp_names,
            index=idx,
            label_visibility="collapsed",
        )
        if selected != current:
            switch_experiment(selected)

    if st.session_state.current_experiment:
        st.caption(f"📁 当前: **{st.session_state.current_experiment}**")
        stats = get_experiment_stats()
        col1, col2 = st.columns(2)
        col1.metric("总条目", stats["total"])
        col2.metric("图表", stats["plot"])
        col3, col4 = st.columns(2)
        col3.metric("原始数据", stats["raw"])
        col4.metric("处理后", stats["processed"])

    st.divider()

    # 导航
    st.subheader("🧭 导航")
    nav_items = {
        "📂 数据浏览器": ("02_Data_Explorer", "🔍 浏览和管理数据"),
        "🔧 流水线构建器": ("03_Plot_Builder", "🔧 构建数据处理流水线"),
        "🔗 流水线": ("04_Pipeline", "⚙️ 流水线与处理器"),
        "🖼️ 图库": ("05_Plot_Gallery", "👁️ 查看已生成图表"),
    }
    for label, (page, hint) in nav_items.items():
        if st.button(label, use_container_width=True, type="tertiary"):
            st.switch_page(f"pages/{page}.py")
    st.caption("💡 提示: 使用 `streamlit run app.py` 启动")

# ---------- 主页内容 ----------
st.title("🏠 FileLine 可视化数据平台")

if not st.session_state.current_experiment:
    st.info(
        "👈 请在侧边栏选择一个实验开始，或通过 CLI 创建实验:\n\n"
        "```bash\npython main.py experiment create <名称>\n"
        "python main.py experiment use <名称>\n```"
    )
else:
    st.success(f"当前实验: **{st.session_state.current_experiment}**")

    stats = get_experiment_stats()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📦 总数据条目", stats["total"])
    col2.metric("📄 原始数据", stats["raw"])
    col3.metric("⚙️ 已处理", stats["processed"])
    col4.metric("📊 图表", stats["plot"])

    st.divider()

    st.subheader("🚀 快速开始")
    quick_col1, quick_col2, quick_col3 = st.columns(3)
    with quick_col1:
        st.markdown("**📂 浏览数据**\n\n查看和管理当前实验中的数据文件，支持过滤和预览。")
        if st.button("前往数据浏览器", use_container_width=True):
            st.switch_page("pages/02_Data_Explorer.py")
    with quick_col2:
        st.markdown("**🔧 处理流水线**\n\n构建多步数据处理流水线，支持分支/合并与中间结果追溯。")
        if st.button("前往流水线构建器", use_container_width=True):
            st.switch_page("pages/03_Plot_Builder.py")
    with quick_col3:
        st.markdown("**🖼️ 浏览结果**\n\n查看所有已生成的数据文件和图表，支持预览和下载。")
        if st.button("前往图库", use_container_width=True):
            st.switch_page("pages/05_Plot_Gallery.py")

    st.divider()

    # 最新数据概览（使用 EntryInfo 安全获取）
    st.subheader("📋 最近数据")
    try:
        with get_session() as session:
            recent = (
                session.query(DataEntry)
                .options(selectinload(DataEntry.tags))
                .order_by(DataEntry.timestamp.desc())
                .limit(5)
                .all()
            )
            rows = []
            for e in recent:
                rows.append(
                    {
                        "ID": e.id,
                        "类型": e.type.upper(),
                        "文件名": os.path.basename(str(e.path)),
                        "时间": e.timestamp.strftime("%m-%d %H:%M"),
                        "标签": ", ".join(t.name for t in e.tags[:3]),
                    }
                )
            import pandas as pd
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    except Exception as ex:
        st.warning(f"无法加载数据: {ex}")
