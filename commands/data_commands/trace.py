# commands/data_commands/trace_tree.py
import click
from sqlalchemy.orm import joinedload
from core.base import get_session
from core.models import DataEntry
import core.storage
from datetime import datetime
from collections import defaultdict
import shutil
from pathlib import Path
import os
import sys

def format_ts(dt: datetime) -> str:
    """统一时间格式化"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")

class TreeNode:
    """树节点结构"""
    __slots__ = ('entry', 'children', 'depth', 'is_last')
    def __init__(self, entry, depth=0):
        self.entry = entry       # 数据条目
        self.children = []       # 子节点列表
        self.depth = depth       # 当前层级
        self.is_last = False     # 是否当前层最后一个节点

def calculate_common_prefix(node: TreeNode) -> Path:
    """递归遍历树结构计算原始文件路径的公共前缀"""
    
    def _collect_paths(n: TreeNode, paths: list):
        """递归收集所有raw类型文件的原始路径"""
        if n.entry.type == 'raw':
            # 转换为绝对路径并标准化
            abs_path = str(Path(n.entry.original_path).resolve())
            paths.append(abs_path)
        for child in n.children:
            _collect_paths(child, paths)
    
    def _find_longest_common(paths: list) -> Path:
        """计算路径列表的最长公共前缀"""
        if not paths:
            return Path()
        
        try:
            common = os.path.commonpath(paths)
            return Path(common)
        except ValueError:
            return Path()
    
    # 递归收集所有原始文件路径
    all_raw_paths = []
    _collect_paths(node, all_raw_paths)
    
    # 计算最长公共前缀
    return _find_longest_common(all_raw_paths)

def export_raw_data(node: TreeNode, export_dir: Path, common_prefix: Path):
    """智能路径导出"""
    if node.entry.type == 'raw':
        original_path = Path(node.entry.original_path)
        try:
            # 计算相对路径
            relative_path = original_path.relative_to(common_prefix)
            dest_path = export_dir / relative_path
            
            # 创建父目录并复制文件
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(original_path, dest_path)
            
            # 显示精简路径
            display_path = '/'.join(relative_path.parts)
            click.secho(f"导出: {display_path}", fg='bright_green')
        except ValueError:
            click.secho(f"路径超出公共前缀: {original_path}", fg='yellow')
        except FileNotFoundError:
            click.secho(f"源文件不存在: {original_path}", fg='red')
        except Exception as e:
            click.secho(f"导出失败: {str(e)}", fg='red')

    for child in node.children:
        export_raw_data(child, export_dir, common_prefix)

def build_tree(entry, max_depth=10, current_depth=0):
    """递归构建树结构"""
    if current_depth >= max_depth:
        click.secho(f"达到最大追溯深度 {max_depth}", fg='yellow')
        return None
    
    node = TreeNode(entry, current_depth)
    # 加载直接父节点（树状结构只展示直接血缘）
    for parent in entry.parents:
        child_node = build_tree(parent, max_depth, current_depth+1)
        if child_node:
            node.children.append(child_node)
    # 标记最后一个子节点
    if node.children:
        node.children[-1].is_last = True
    return node

def print_tree(node, prefix='', is_last=False):
    """递归打印树结构"""
    connectors = {
        'space':  '    ',
        'branch': '├── ',
        'end':    '└── ',
        'vert':   '│   '
    }
    
    # 当前节点信息
    current_line = f"{click.style(prefix + (connectors['end'] if is_last else connectors['branch']), fg='cyan')}"
    current_line += format_node(node)
    click.echo(current_line)
    if node.entry.type == 'processed':
        click.echo(f"{prefix}└── {click.style(f'处理信息: {node.entry.description}', fg='bright_magenta')}")
    
    # 子节点前缀计算
    new_prefix = prefix + (connectors['space'] if is_last else connectors['vert'])
    
    # 递归子节点
    for i, child in enumerate(node.children):
        is_last_child = (i == len(node.children)-1)
        print_tree(child, new_prefix, is_last_child)

def format_node(node: TreeNode) -> str:
    """格式化单个节点信息"""
    tags = ', '.join(t.name for t in node.entry.tags) if node.entry.tags else "无"
    if node.entry.type != 'raw':
        return (
            f"{node.entry.id} │ "
            f"{click.style(node.entry.type.upper(), fg='bright_magenta')} │ "
            f"路径: {click.style(node.entry.path, fg='bright_blue')} │ "
            f"时间: {format_ts(node.entry.timestamp)}"
        )
    else:
        return (
            f"{node.entry.id} │ "
            f"{click.style(node.entry.type.upper(), fg='bright_magenta')} │ "
            f"路径: {click.style(node.entry.original_path, fg='bright_blue')} │ "
            f"时间: {format_ts(node.entry.timestamp)} │ "
        )

@click.command()
@click.option("--id", "-i", required=True, type=int, help="要追溯的最终输出ID")
@click.option("--depth", "-d", default=5, show_default=True, help="最大追溯深度")
@click.option("--export-dir", 
             type=click.Path(file_okay=False, dir_okay=True, writable=True),
             help="导出原始数据的目录路径")
def trace_cmd(id, depth, export_dir):
    """以树状结构展示数据血缘"""
    with get_session() as session:
        # 预加载父节点关系
        entry = session.query(DataEntry).options(
            joinedload(DataEntry.parents),
            joinedload(DataEntry.tags)
        ).get(id)
        
        if not entry:
            click.secho(f"错误：ID {id} 不存在", fg="red")
            return
            
        # 构建树结构
        root = build_tree(entry, max_depth=depth)
        if not root:
            click.secho("无法构建追溯树", fg='yellow')
            return
            
        # 打印树状结构
        click.secho(f"\n▼ 数据血缘树 (ID: {id})", fg='bright_green', bold=True)
        print_tree(root)
        click.echo("\n" + "━" * 80)
        click.secho("图例说明:", fg='bright_white')
        click.echo(f"  {click.style('RAW', fg='bright_magenta')} - 原始数据")
        click.echo(f"  {click.style('PROCESSED', fg='bright_magenta')} - 处理数据")
        click.echo(f"  {click.style('PLOT', fg='bright_magenta')} - 图表文件")


        # 导出原始数据
        if export_dir:
            export_path = Path(export_dir)
            # 若是相对路径，则相对于core.storage.FileStorage.base_path
            if not export_path.is_absolute():
                export_path = Path(core.storage.FileStorage().base_path) / "trace" / export_path
            common_prefix = calculate_common_prefix(root)
            try:
                export_path.mkdir(parents=True, exist_ok=True)
                click.secho("\n开始导出原始数据...", fg='bright_white')
                export_raw_data(root, export_path, common_prefix)
                click.secho(f"\n导出完成！文件保存在: {export_path.resolve()}", fg='bright_green', bold=True)
            except PermissionError:
                click.secho("错误：没有写入目标目录的权限", fg='red')
            except Exception as e:
                click.secho(f"导出异常: {str(e)}", fg='red')
        
        click.echo("\n" + "━" * 80)