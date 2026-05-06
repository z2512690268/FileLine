"""FileLine - 数据浏览器"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
from app_utils import (
    get_data_entries,
    get_entry_dataframe,
    get_all_tags_cached,
    format_file_size,
)

st.set_page_config(page_title="数据浏览器", page_icon="📂", layout="wide")

st.title("📂 数据浏览器")
st.caption("浏览和管理当前实验中的所有数据条目")

# ---------- 过滤器 ----------
with st.expander("🔍 过滤条件", expanded=True):
    col1, col2, col3 = st.columns(3)
    with col1:
        type_filter = st.selectbox(
            "数据类型", options=["全部", "raw", "processed", "plot"], index=0
        )
    with col2:
        tags = get_all_tags_cached()
        tag_filter = st.multiselect("标签过滤", options=tags if tags else [])
    with col3:
        limit = st.number_input(
            "显示数量", min_value=5, max_value=200, value=50, step=10
        )

# ---------- 数据查询 ----------
entry_type = None if type_filter == "全部" else type_filter
entries = get_data_entries(
    entry_type=entry_type, tags=tag_filter if tag_filter else None, limit=limit
)

st.divider()
st.subheader(f"📋 共 {len(entries)} 条记录")

if not entries:
    st.info("暂无匹配的数据条目。请通过 CLI 添加数据: `python main.py data add <文件路径>`")
else:
    # 概览表格
    rows = []
    for e in entries:
        path = Path(e.path)
        rows.append(
            {
                "ID": e.id,
                "类型": e.type.upper(),
                "文件名": path.name if path else "-",
                "大小": format_file_size(str(e.path)) if e.path else "-",
                "标签": ", ".join(e.tags[:5]),
                "时间": e.timestamp.strftime("%Y-%m-%d %H:%M"),
            }
        )
    df_overview = pd.DataFrame(rows)
    event = st.dataframe(
        df_overview,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "ID": st.column_config.NumberColumn(width=60),
            "类型": st.column_config.TextColumn(width=80),
            "文件名": st.column_config.TextColumn(width=300),
        },
    )

    # 选取选中的行
    if event.selection and event.selection.rows:
        idx = event.selection.rows[0]
        selected = entries[idx]

        st.divider()
        st.subheader(f"📄 详情 - ID: {selected.id}")

        detail_col1, detail_col2 = st.columns(2)
        with detail_col1:
            st.markdown(f"**路径:** `{selected.path}`")
            st.markdown(f"**原始路径:** `{selected.original_path or '-'}`")
            st.markdown(
                f"**父记录 ID:** {', '.join(str(pid) for pid in selected.parent_ids) or '无'}"
            )
        with detail_col2:
            st.markdown(f"**类型:** {selected.type.upper()}")
            st.markdown(f"**时间:** {selected.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            st.markdown(
                f"**标签:** {', '.join(selected.tags) or '无'}"
            )

        # 数据预览
        df = get_entry_dataframe(selected.id)
        if df is not None:
            st.divider()
            st.subheader("👁️ 数据预览")
            with st.expander("展开数据预览", expanded=True):
                st.dataframe(df, width="stretch")
                st.caption(f"Shape: {df.shape[0]} rows × {df.shape[1]} cols")

                # CSV 下载
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="📥 下载 CSV",
                    data=csv,
                    file_name=f"entry_{selected.id}.csv",
                    mime="text/csv",
                )

                # 简易统计 (仅数值列)
                num_cols = df.select_dtypes(include="number").columns.tolist()
                if num_cols:
                    with st.expander("📊 数值列统计"):
                        st.dataframe(df[num_cols].describe(), width="stretch")
        else:
            st.warning("⚠️ 无法预览此文件（文件可能已被移动或格式不支持）")
