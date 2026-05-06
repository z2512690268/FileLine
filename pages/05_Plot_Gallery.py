"""FileLine - 图库"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
from sqlalchemy.orm import selectinload
from app_utils import format_file_size, build_provenance_tree, render_provenance_ui
from core.base import get_session
from core.models import DataEntry, Tag

st.set_page_config(page_title="图库", page_icon="🖼️", layout="wide")

st.title("🖼️ 图库")
st.caption("浏览所有已生成的数据文件和图表")

# 获取所有带 auto_plot 标签的已处理条目
with get_session() as session:
    entries = (
        session.query(DataEntry)
        .options(selectinload(DataEntry.tags))
        .filter(DataEntry.type == "processed")
        .order_by(DataEntry.timestamp.desc())
        .all()
    )

# 把有 auto_plot 标签的排在前面
auto_entries = [e for e in entries if any(t.name == "auto_plot" for t in e.tags)]
other_entries = [e for e in entries if e not in auto_entries]
display_entries = auto_entries + other_entries

if not display_entries:
    st.info("暂无数据。请前往「流水线构建器」生成数据。")
else:
    # 概览表格
    rows = []
    for e in display_entries:
        path = Path(e.path)
        rows.append(
            {
                "ID": e.id,
                "文件名": path.name if path else "-",
                "格式": path.suffix.upper().lstrip(".") if path else "-",
                "大小": format_file_size(str(e.path)) if e.path else "-",
                "标签": ", ".join(t.name for t in e.tags[:5]),
                "时间": e.timestamp.strftime("%m-%d %H:%M"),
            }
        )
    df_overview = pd.DataFrame(rows)
    event = st.dataframe(
        df_overview,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    if event.selection and event.selection.rows:
        idx = event.selection.rows[0]
        selected = display_entries[idx]
        file_path = Path(selected.path)

        st.divider()
        st.subheader(f"🖼️ {file_path.name} (ID: {selected.id})")

        col1, col2 = st.columns([3, 1])
        with col1:
            if file_path.exists():
                ext = file_path.suffix.lower()
                if ext in (".png", ".jpg", ".jpeg"):
                    st.image(str(file_path), use_container_width=True)
                elif ext == ".pdf":
                    try:
                        import pdfplumber
                        with pdfplumber.open(str(file_path)) as pdf:
                            if pdf.pages:
                                img = pdf.pages[0].to_image(resolution=200)
                                st.image(img.original, use_container_width=True, caption="预览（PDF 第一页）")
                        with open(file_path, "rb") as f:
                            st.download_button(
                                "📥 下载 PDF", f, file_name=file_path.name, mime="application/pdf"
                            )
                    except ImportError:
                        st.info("PDF 预览需要 pdfplumber: `pip install pdfplumber`")
                        with open(file_path, "rb") as f:
                            st.download_button(
                                "📥 下载 PDF", f, file_name=file_path.name, mime="application/pdf"
                            )
                elif ext == ".svg":
                    with open(file_path) as f:
                        st.image(f.read(), use_container_width=True)
                else:
                    st.warning("此文件格式无法直接预览")
            else:
                st.error("文件不存在")

        with col2:
            st.markdown("**标签:**")
            for t in selected.tags:
                st.markdown(f"- `{t.name}`")
            st.markdown(f"**时间:**\n{selected.timestamp.strftime('%Y-%m-%d %H:%M')}")
            st.markdown(f"**路径:**\n`{selected.path}`")

        # 血缘追溯
        st.divider()
        with st.expander("🔗 处理链追溯 / Provenance", expanded=False):
            tree = build_provenance_tree(selected.id)
            if tree:
                render_provenance_ui(tree)
            else:
                st.caption("无祖先数据（原始数据）")
