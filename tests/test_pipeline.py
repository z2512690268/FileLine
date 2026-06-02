"""PipelineRunner: 初始加载/步骤执行/缓存/导出"""
import shutil
from pathlib import Path
from core.pipeline import PipelineRunner, InitialLoadConfig, IncludeSpec, PipelineStep
from core.storage import FileStorage
from core.processing import ProcessorRegistry, DataProcessor
from core.models import DataEntry
import pandas as pd


@ProcessorRegistry.register("_test_pipe_add", input_type="single", output_ext=".csv")
def _adder(input_path, output_path, inc=1):
    df = pd.read_csv(input_path.path)
    df["v"] = df["v"] + inc
    df.to_csv(output_path, index=False)
    return "added"


@ProcessorRegistry.register("_test_multi_pipe", input_type="single", output_type="multi", output_ext="")
def _split_pipe(input_path, output_dir, split_col="cat"):
    df = pd.read_csv(input_path.path)
    for val in df[split_col].unique():
        fname = f"{val}.csv"
        df[df[split_col] == val].to_csv(output_dir / fname, index=False)
        yield (fname,)


class TestInitialLoad:
    def test_load_single_pattern(self, storage, db_session, test_experiment, tmp_path):
        # 创建几个测试文件
        (tmp_path / "data_a.csv").write_text("x,1\n")
        (tmp_path / "data_b.csv").write_text("y,2\n")
        (tmp_path / "ignore.txt").write_text("skip")

        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        ids = runner._load_initial_files(config)
        assert len(ids) == 2
        # 文件已复制到 raw/ 且以 ID 命名
        for eid in ids:
            entry = db_session.get(DataEntry, eid)
            assert entry is not None
            assert Path(entry.path).stem == str(eid)
            assert Path(entry.path).exists()

    def test_original_path_set(self, storage, db_session, test_experiment, tmp_path):
        f = tmp_path / "origin.csv"
        f.write_text("v,1\n")
        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        ids = runner._load_initial_files(config)
        entry = db_session.get(DataEntry, ids[0])
        assert entry.original_path == str(f)

    def test_exclude_pattern(self, storage, db_session, test_experiment, tmp_path):
        (tmp_path / "keep.csv").write_text("a,1\n")
        (tmp_path / "skip.csv").write_text("b,2\n")
        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
            exclude_patterns=["*skip*"],
        )
        ids = runner._load_initial_files(config)
        assert len(ids) == 1
        entry = db_session.get(DataEntry, ids[0])
        assert "skip" not in entry.path

    def test_mtime_cache_hit(self, storage, db_session, test_experiment, tmp_path):
        f = tmp_path / "cached.csv"
        f.write_text("v,1\n")
        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        ids1 = runner._load_initial_files(config)
        # 第二次加载应命中 mtime 缓存
        ids2 = runner._load_initial_files(config)
        assert ids1 == ids2

    def test_mtime_cache_miss_on_change(self, storage, db_session, test_experiment, tmp_path):
        f = tmp_path / "changed.csv"
        f.write_text("v,1\n")
        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        ids1 = runner._load_initial_files(config)
        # 修改文件
        f.write_text("v,99\n")
        ids2 = runner._load_initial_files(config)
        # 新 ID, 旧数据仍存在
        assert ids2[0] != ids1[0]

    def test_no_match_raises(self, storage, db_session, test_experiment, tmp_path):
        import pytest
        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.nonexistent"))],
        )
        with pytest.raises(FileNotFoundError):
            runner._load_initial_files(config)

    def test_load_with_tags(self, storage, db_session, test_experiment, tmp_path):
        (tmp_path / "tagged.csv").write_text("v,1\n")
        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"), tags=["auto_ingest"])],
            tags=["experiment_1"],
        )
        ids = runner._load_initial_files(config)
        entry = db_session.get(DataEntry, ids[0])
        tag_names = {t.name for t in entry.tags}
        assert "auto_ingest" in tag_names
        assert "experiment_1" in tag_names


class TestPipelineExecute:
    def test_single_step(self, storage, db_session, test_experiment, tmp_path):
        f = tmp_path / "base.csv"
        f.write_text("v\n1\n")
        runner = PipelineRunner(storage, db_session)
        load_cfg = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        steps = [
            PipelineStep(processor="_test_pipe_add", inputs="initial", params={"inc": 10},
                         output_var="step1", cache=False, force_rerun=False, export=None),
        ]
        ctx = runner.execute(load_cfg, steps)
        assert "step1" in ctx
        assert len(ctx["step1"]) == 1
        entry = db_session.get(DataEntry, ctx["step1"][0])
        assert entry is not None
        assert "added" in [t.name for t in entry.tags]
        df = pd.read_csv(entry.path)
        assert list(df["v"]) == [11], f"实际值: {list(df['v'])}"

    def test_two_steps(self, storage, db_session, test_experiment, tmp_path):
        import pandas as pd
        f = tmp_path / "two.csv"
        f.write_text("v\n1\n")
        runner = PipelineRunner(storage, db_session)
        load_cfg = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        steps = [
            PipelineStep(processor="_test_pipe_add", inputs="initial", params={"inc": 1},
                         output_var="s1", cache=False, force_rerun=False, export=None),
            PipelineStep(processor="_test_pipe_add", inputs="s1", params={"inc": 100},
                         output_var="s2", cache=False, force_rerun=False, export=None),
        ]
        ctx = runner.execute(load_cfg, steps)
        entry2 = db_session.get(DataEntry, ctx["s2"][0])
        assert entry2 is not None and Path(entry2.path).exists()
        df = pd.read_csv(entry2.path)
        assert list(df["v"]) == [102]

    def test_export(self, storage, db_session, test_experiment, tmp_path):
        f = tmp_path / "export.csv"
        f.write_text("v,5\n")
        runner = PipelineRunner(storage, db_session)
        load_cfg = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        steps = [
            PipelineStep(processor="_test_pipe_add", inputs="initial", params={"inc": 0},
                         output_var="ex", cache=False, force_rerun=False, export="result"),
        ]
        runner.execute(load_cfg, steps)
        export_path = storage.base_path / "exports" / "result"
        assert export_path.exists()

    def test_multi_output_in_pipeline(self, storage, db_session, test_experiment, tmp_path):
        f = tmp_path / "multi_test.csv"
        f.write_text("cat,v\nX,1\nY,2\nX,3\n")
        runner = PipelineRunner(storage, db_session)
        load_cfg = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        steps = [
            PipelineStep(processor="_test_multi_pipe", inputs="initial", params={"split_col": "cat"},
                         output_var="split", cache=False, force_rerun=False, export=None),
        ]
        ctx = runner.execute(load_cfg, steps)
        assert len(ctx["split"]) == 2  # X, Y

    def test_multi_output_feed_downstream(self, storage, db_session, test_experiment, tmp_path):
        """多输出结果可作为下游 multi 输入的源"""
        f = tmp_path / "feed.csv"
        f.write_text("cat,v\nA,10\nB,20\nA,30\n")
        runner = PipelineRunner(storage, db_session)
        load_cfg = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        steps = [
            PipelineStep(processor="_test_multi_pipe", inputs="initial", params={"split_col": "cat"},
                         output_var="split", cache=False, force_rerun=False, export=None),
        ]
        ctx = runner.execute(load_cfg, steps)
        # 下游步骤可以用 split 作为 multi 输入
        assert len(ctx["split"]) == 2
        # 验证数据: 实际 A 有 2 行, B 有 1 行
        paths = {}
        for eid in ctx["split"]:
            entry = db_session.get(DataEntry, eid)
            tag_names = {t.name for t in entry.tags}
            paths[eid] = entry.path
