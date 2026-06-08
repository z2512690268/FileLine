import json
from pathlib import Path

import click

from core.global_sets import global_set_affected, list_global_sets, read_global_set, resolve_text


@click.group(name="globals")
def global_sets_cmd():
    """管理全局变量组/样式预设。"""
    pass


@global_sets_cmd.command("list")
def list_sets():
    """列出当前实验可用的变量组。"""
    items = list_global_sets()
    if not items:
        click.secho("暂无全局变量组", fg="yellow")
        return
    for item in items:
        scope = item.get("scope", "")
        count = item.get("variableCount", 0)
        desc = item.get("description", "")
        click.echo(f"{item['id']:<24s} {scope:<10s} {count:>2} vars  {desc}")


@global_sets_cmd.command("show")
@click.argument("name")
def show_set(name):
    """查看变量组内容。"""
    item = read_global_set(name)
    click.echo(item["text"])


@global_sets_cmd.command("affected")
@click.argument("name")
def affected(name):
    """列出会用到该变量组变量的 pipeline。"""
    rows = global_set_affected(name)
    if not rows:
        click.secho("没有发现引用这些变量的 pipeline", fg="yellow")
        return
    for row in rows:
        click.echo(f"{row['path']}: {', '.join(row['variables'])}")


@global_sets_cmd.command("resolve")
@click.argument("pipeline_yaml", type=click.Path(exists=True))
@click.argument("name")
def resolve(pipeline_yaml, name):
    """预览变量组应用后的 pipeline YAML。"""
    item = read_global_set(name)
    text = Path(pipeline_yaml).read_text(encoding="utf-8")
    result = resolve_text(text, item["values"])
    click.echo(result["resolvedText"])
    if result["missing"]:
        click.secho(f"未替换变量: {', '.join(result['missing'])}", fg="yellow", err=True)
    click.echo(json.dumps({"used": result["used"], "missing": result["missing"]}, ensure_ascii=False), err=True)
