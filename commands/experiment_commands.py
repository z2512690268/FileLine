# 新增 commands/experiment_commands.py
import click
from pathlib import Path
from tabulate import tabulate
from core.base import experiment_manager

@click.group()
def experiment():
    """实验管理命令"""
    click.echo(f"当前实验环境 ：{experiment_manager.current_experiment}")
    pass

@experiment.command()
@click.argument("name")
@click.option("--description", help="实验描述")
def create(name, description):
    """创建新实验"""
    try:
        experiment_manager.create(name, description)
        click.secho(f"成功创建实验: {name}", fg='green')
    except Exception as e:
        click.secho(f"创建失败: {str(e)}", fg='red')

@experiment.command()
@click.argument("name")
def use(name):
    """切换当前实验"""
    try:
        experiment_manager.set_current(name)
        click.secho(f"已切换到实验: {name}", fg='green')
    except ValueError as e:
        click.secho(str(e), fg='red')

@experiment.command()
def list():
    """列出所有实验"""
    experiments = experiment_manager.get_experiments()
    table = []
    for name, config in experiments.items():
        table.append([
            name,
            config['description'],
            Path(config['database']).parent,
            config['created_at']
        ])
    click.echo(tabulate(table, 
        headers=["名称", "描述", "目录", "创建时间"],
        tablefmt="fancy_grid"))

@experiment.command()
@click.argument("name")
def delete(name):
    """删除实验"""
    try:
        experiments = experiment_manager.get_experiments()
        if name not in experiments:
            raise ValueError(f"实验 {name} 不存在")
        config = experiments[name]
        exp_dir = Path(config['data_root'])
        if not exp_dir.is_absolute():
            exp_dir = experiment_manager.project_root / exp_dir
        if exp_dir.exists():
            import shutil
            shutil.rmtree(exp_dir)
        del experiments[name]
        experiment_manager._save_experiments(experiments)
        if experiment_manager.current_experiment == name:
            experiment_manager.delete_current()
        click.secho(f"成功删除实验: {name}", fg='green')
    except Exception as e:
        click.secho(f"删除失败: {str(e)}", fg='red')


@experiment.command()
@click.argument("name")
@click.argument("key")
@click.argument("value")
def config(name, key, value):
    """修改实验配置 (如 source_mode)"""
    exps = experiment_manager.get_experiments()
    if name not in exps:
        click.secho(f"实验 {name} 不存在", fg="red")
        return
    if key not in ("source_mode", "description"):
        click.secho(f"不支持配置项: {key} (支持: source_mode, description)", fg="red")
        return
    exps[name][key] = value
    experiment_manager._save_experiments(exps)
    click.secho(f"实验 {name} 的 {key} 已设为 {value}", fg="green")


@experiment.command()
@click.argument("name")
@click.option("--pipelines", help="绑定的 pipeline 文件 (glob, 如 'pipelines/*.yaml')")
@click.option("--processors", help="绑定的 experiment-specific processor (glob, 如 'processes/*.py')")
@click.option("--output", "-o", default=None, help="导出文件路径 (默认: ./<name>.flxp)")
def export(name, pipelines, processors, output):
    """导出实验为可移植包 (.flxp) 包含数据/数据库/pipeline"""
    import tarfile
    import json
    from datetime import datetime

    exps = experiment_manager.get_experiments()
    if name not in exps:
        click.secho(f"实验 {name} 不存在", fg="red")
        return

    exp_dir = experiment_manager.project_root / "experiments" / name
    if not exp_dir.exists():
        click.secho(f"实验目录不存在: {exp_dir}", fg="red")
        return

    # 收集 pipeline 文件
    pipeline_files = []
    if pipelines:
        import glob as _glob
        pipeline_files = _glob.glob(pipelines, recursive=True)
        if not pipeline_files:
            click.secho(f"未找到 pipeline 文件: {pipelines}", fg="yellow")

    # 收集 experiment-specific processor 文件
    processor_files = []
    if processors:
        import glob as _glob
        processor_files = _glob.glob(processors, recursive=True)
        if not processor_files:
            click.secho(f"未找到 processor 文件: {processors}", fg="yellow")

    output_path = Path(output or f"{name}.flxp")
    old_project_root = str(experiment_manager.project_root)

    with tarfile.open(str(output_path), "w:gz") as tar:
        # 添加数据目录 (raw + processed + exports, 排除 .db 和 .meta)
        for subdir in ["raw", "processed", "exports"]:
            src = exp_dir / subdir
            if src.exists():
                arc_base = f"experiment/{subdir}"
                for f in src.rglob("*"):
                    if f.is_file() and f.suffix not in (".meta",):
                        tar.add(str(f), arcname=f"{arc_base}/{f.relative_to(src)}")

        # 添加数据库
        db_path = exp_dir / f"{name}.db"
        if db_path.exists():
            arc_db = "experiment/data.db"
            # 临时复制 DB，改写路径为相对路径
            import shutil, sqlite3
            tmp_db = exp_dir / f"{name}_tmp_export.db"
            shutil.copy(str(db_path), str(tmp_db))
            try:
                conn = sqlite3.connect(str(tmp_db))
                old_prefix = f"{old_project_root}/experiments/{name}/"
                new_prefix = f"{{PROJECT_ROOT}}/experiments/{name}/"
                for tbl, col in [("data_entries", "path"), ("file_mtime_cache", "file_path")]:
                    conn.execute(
                        f"UPDATE {tbl} SET {col} = REPLACE({col}, ?, ?) "
                        f"WHERE {col} LIKE ?",
                        (old_prefix, new_prefix, f"{old_prefix}%")
                    )
                # 保留 original_path 不变
                conn.commit()
                conn.close()
                tar.add(str(tmp_db), arcname=arc_db)
            finally:
                tmp_db.unlink(missing_ok=True)

        # 添加 pipeline 文件
        for pf in pipeline_files:
            arc_name = f"pipelines/{Path(pf).name}"
            tar.add(pf, arcname=arc_name)

        # 添加 experiment-specific processor 文件
        for pf in processor_files:
            arc_name = f"processors/{Path(pf).name}"
            tar.add(pf, arcname=arc_name)

        # manifest
        manifest = {
            "type": "fileline-experiment",
            "version": 1,
            "name": name,
            "exported_at": datetime.now().isoformat(),
            "old_project_root": old_project_root,
            "pipeline_files": [Path(pf).name for pf in pipeline_files] if pipeline_files else [],
            "processor_files": [Path(pf).name for pf in processor_files] if processor_files else [],
        }
        import io
        manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode()
        info = tarfile.TarInfo(name="manifest.json")
        info.size = len(manifest_bytes)
        tar.addfile(info, io.BytesIO(manifest_bytes))

    click.secho(f"导出成功: {output_path} ({output_path.stat().st_size / 1024:.0f} KB)", fg="green")
    click.secho(f"  包含 {len(pipeline_files)} 个 pipeline 文件" if pipeline_files else "  未包含 pipeline 文件", fg="cyan")


@experiment.command()
@click.argument("package", type=click.Path(exists=True))
@click.option("--name", default=None, help="导入后重命名实验 (默认使用原名)")
@click.option("--pipelines-dir", default="FileLine-Pipelines", help="pipeline 文件导入目录")
def import_cmd(package, name, pipelines_dir):
    """导入 .flxp 实验包"""
    import tarfile
    import json
    import shutil
    import sqlite3

    click.echo(f"正在导入: {package}")

    with tarfile.open(str(package), "r:gz") as tar:
        # 读取 manifest
        mf = tar.extractfile("manifest.json")
        if mf is None:
            click.secho("无效的包: 缺少 manifest.json", fg="red")
            return
        manifest = json.loads(mf.read().decode())
        exp_name = name or manifest["name"]

        # 检查是否已存在
        exps = experiment_manager.get_experiments()
        if exp_name in exps:
            click.secho(f"实验 {exp_name} 已存在", fg="red")
            return

        old_root = manifest["old_project_root"]
        new_root = str(experiment_manager.project_root)

        # 创建实验 (设置为 raw 模式: 使用缓存数据而非原始路径)
        experiment_manager.create(exp_name, f"从 {Path(package).name} 导入")
        exps = experiment_manager.get_experiments()
        if exp_name in exps:
            exps[exp_name]["source_mode"] = "raw"
            experiment_manager._save_experiments(exps)
        # 如果当前实验就是导入目标, 先切走避免 init_db() 失败
        if experiment_manager.current_experiment == exp_name:
            temp_name = f"_tmp_{exp_name}"
            try:
                experiment_manager.create(temp_name, "temp")
                experiment_manager.set_current(temp_name)
            except Exception:
                pass
        exp_dir = experiment_manager.project_root / "experiments" / exp_name
        (exp_dir / "raw").mkdir(exist_ok=True)
        (exp_dir / "processed").mkdir(exist_ok=True)
        (exp_dir / "exports").mkdir(exist_ok=True)

        # 解压数据文件 (跳过 data.db, 由下面单独处理)
        for m in tar.getmembers():
            if m.name.startswith("experiment/") and m.isfile() and m.name != "experiment/data.db":
                rel = "/".join(m.name.split("/")[1:])
                target = exp_dir / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(m) as src_f:
                    with open(str(target), "wb") as dst_f:
                        import shutil as _sh
                        _sh.copyfileobj(src_f, dst_f)

        # 恢复数据库并改写路径
        db_member = next((m for m in tar.getmembers() if m.name == "experiment/data.db"), None)
        if db_member:
            db_target = exp_dir / f"{exp_name}.db"
            with tar.extractfile(db_member) as src_f:
                with open(str(db_target), "wb") as dst_f:
                    shutil.copyfileobj(src_f, dst_f)

            # 改写路径 (只改 path/file_path, 不改 original_path)
            conn = sqlite3.connect(str(db_target))
            old_name = manifest["name"]
            for tbl, col in [("data_entries", "path"), ("file_mtime_cache", "file_path")]:
                old_prefix = f"{{PROJECT_ROOT}}/experiments/{old_name}/"
                new_prefix = f"{new_root}/experiments/{exp_name}/"
                conn.execute(
                    f"UPDATE {tbl} SET {col} = REPLACE({col}, ?, ?) "
                    f"WHERE {col} LIKE ?",
                    (old_prefix, new_prefix, f"{old_prefix}%")
                )
                if old_root != new_root or old_name != exp_name:
                    old_abs = f"{old_root}/experiments/{old_name}/"
                    new_abs = f"{new_root}/experiments/{exp_name}/"
                    conn.execute(
                        f"UPDATE {tbl} SET {col} = REPLACE({col}, ?, ?) "
                        f"WHERE {col} LIKE ?",
                        (old_abs, new_abs, f"{old_abs}%")
                    )
            conn.commit()
            # 迁移: 确保 step_cache 有 group_name 列
            try:
                conn.execute("ALTER TABLE step_cache ADD COLUMN group_name VARCHAR(64)")
                conn.commit()
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE step_cache ADD COLUMN cache_scope VARCHAR(64)")
                conn.commit()
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE pipeline_versions ADD COLUMN cache_scope VARCHAR(64)")
                conn.commit()
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE pipeline_versions ADD COLUMN config_snapshot TEXT")
                conn.commit()
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE pipeline_versions ADD COLUMN processor_snapshot TEXT")
                conn.commit()
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE pipeline_versions ADD COLUMN result_hash VARCHAR(64)")
                conn.commit()
            except Exception:
                pass
            try:
                conn.execute("UPDATE step_cache SET cache_scope = 'legacy' WHERE cache_scope IS NULL OR cache_scope = ''")
                conn.execute("UPDATE pipeline_versions SET cache_scope = 'legacy' WHERE cache_scope IS NULL OR cache_scope = ''")
                conn.commit()
            except Exception:
                pass
            conn.close()

        # 解压 pipeline 文件 (到实验目录内, 自包含)
        pip_exp_target = exp_dir / "pipelines"
        for m in tar.getmembers():
            if m.name.startswith("pipelines/") and m.isfile():
                fname = "/".join(m.name.split("/")[1:])
                for target in [pip_exp_target / fname]:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with tar.extractfile(m) as src_f:
                        with open(str(target), "wb") as dst_f:
                            shutil.copyfileobj(src_f, dst_f)

        # 解压 experiment-specific processor 文件
        proc_target = exp_dir / "processors"
        for m in tar.getmembers():
            if m.name.startswith("processors/") and m.isfile():
                fname = "/".join(m.name.split("/")[1:])
                target = proc_target / fname
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(m) as src_f:
                    with open(str(target), "wb") as dst_f:
                        shutil.copyfileobj(src_f, dst_f)

        click.secho(f"导入成功: {exp_name}", fg="green")
        click.secho(f"  python main.py experiment use {exp_name}", fg="cyan")
        if pip_exp_target.exists():
            click.secho(f"  pipeline: {pip_exp_target}/", fg="cyan")
