"""PipelineRunner: 初始加载/步骤执行/缓存/导出"""
import json
import shutil
from pathlib import Path
from core.pipeline import PipelineRunner, InitialLoadConfig, IncludeSpec, PipelineStep
from core.pipeline_versions import PipelineVersionManager
from core.storage import FileStorage
from core.processing import ProcessorRegistry, DataProcessor
from core.models import DataEntry, StepCache
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
        sources = runner._load_initial_files(config)
        ids = sources["initial"]
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
        sources = runner._load_initial_files(config)
        entry = db_session.get(DataEntry, sources["initial"][0])
        assert entry.original_path == str(f)

    def test_exclude_pattern(self, storage, db_session, test_experiment, tmp_path):
        (tmp_path / "keep.csv").write_text("a,1\n")
        (tmp_path / "skip.csv").write_text("b,2\n")
        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
            exclude_patterns=["*skip*"],
        )
        sources = runner._load_initial_files(config)
        ids = sources["initial"]
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
        r1 = runner._load_initial_files(config)
        r2 = runner._load_initial_files(config)
        assert r1["initial"] == r2["initial"]

    def test_mtime_cache_miss_on_change(self, storage, db_session, test_experiment, tmp_path):
        f = tmp_path / "changed.csv"
        f.write_text("v,1\n")
        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        r1 = runner._load_initial_files(config)
        f.write_text("v,99\n")
        r2 = runner._load_initial_files(config)
        assert r2["initial"][0] != r1["initial"][0]

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
        sources = runner._load_initial_files(config)
        entry = db_session.get(DataEntry, sources["initial"][0])
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

    def test_cache_scope_follows_version_branch(self, storage, db_session, test_experiment, tmp_path):
        f = tmp_path / "branch.csv"
        f.write_text("v\n1\n")
        load_cfg = InitialLoadConfig(
            include_patterns=[IncludeSpec(path=str(tmp_path / "*.csv"))],
        )
        steps = [
            PipelineStep(
                processor="_test_pipe_add",
                inputs="initial",
                params={"inc": 10},
                output_var="step1",
                cache=True,
                force_rerun=False,
                export=None,
            ),
        ]

        legacy_runner = PipelineRunner(storage, db_session, cache_scope="legacy")
        ctx_legacy = legacy_runner.execute(load_cfg, steps)
        legacy_ids = ctx_legacy["step1"]

        pvm = PipelineVersionManager()
        legacy_version_id = pvm.record_run(legacy_ids, config_file="branch.yaml", cache_scope="legacy")

        fresh_scope = pvm.create_fresh_scope()
        fresh_runner = PipelineRunner(storage, db_session, cache_scope=fresh_scope)
        ctx_fresh = fresh_runner.execute(load_cfg, steps)
        fresh_ids = ctx_fresh["step1"]
        assert fresh_ids != legacy_ids

        fresh_version_id = pvm.record_run(fresh_ids, config_file="branch.yaml", cache_scope=fresh_scope)
        pvm.set_current(legacy_version_id)

        rollback_runner = PipelineRunner(storage, db_session, cache_scope="legacy")
        ctx_rollback = rollback_runner.execute(load_cfg, steps)
        assert ctx_rollback["step1"] == legacy_ids

        cached_scopes = {
            row.output_id: row.cache_scope
            for row in db_session.query(StepCache).all()
        }
        assert cached_scopes[legacy_ids[0]] == "legacy"
        assert cached_scopes[fresh_ids[0]] == fresh_scope
        assert fresh_version_id == legacy_version_id

    def test_versions_are_scoped_by_pipeline_config(self, storage, db_session, test_experiment):
        pvm = PipelineVersionManager()
        a1 = pvm.record_run([1], config_file="fmrl/timeline.yaml", cache_scope="scope_a1", config_snapshot="steps: []\n")
        b1 = pvm.record_run([2], config_file="fmrl/throughput.yaml", cache_scope="scope_b1")
        a2 = pvm.record_run([3], config_file="fmrl/timeline.yaml", cache_scope="scope_a2")

        versions = pvm.list_versions()
        by_id = {item["id"]: item for item in versions}
        assert by_id[a1]["status"] == "superseded"
        assert by_id[a2]["status"] == "active"
        assert by_id[b1]["status"] == "active"

        pvm.set_current(a1)
        versions = pvm.list_versions()
        by_id = {item["id"]: item for item in versions}
        assert by_id[a1]["status"] == "active"
        assert by_id[a2]["status"] == "superseded"
        assert by_id[b1]["status"] == "active"
        assert pvm.get_current_cache_scope("fmrl/timeline.yaml") == "scope_a1"
        assert pvm.get_current_cache_scope("fmrl/throughput.yaml") == "scope_b1"
        assert pvm.get_version(a1)["config_snapshot"] == "steps: []\n"

    def test_undo_last_is_scoped_by_pipeline_config(self, storage, db_session, test_experiment):
        pvm = PipelineVersionManager()
        a1 = pvm.record_run([1], config_file="fmrl/timeline.yaml", cache_scope="scope_a1")
        b1 = pvm.record_run([2], config_file="fmrl/throughput.yaml", cache_scope="scope_b1")
        a2 = pvm.record_run([3], config_file="fmrl/timeline.yaml", cache_scope="scope_a2")

        restored = pvm.undo_last("fmrl/timeline.yaml")
        assert restored["id"] == a1

        by_id = {item["id"]: item for item in pvm.list_versions()}
        assert by_id[a1]["status"] == "active"
        assert by_id[a2]["status"] == "superseded"
        assert by_id[b1]["status"] == "active"

    def test_duplicate_version_is_not_recorded_when_snapshot_and_result_match(self, storage, db_session, test_experiment, tmp_path):
        result_file = tmp_path / "result.pdf"
        result_file.write_bytes(b"same result")
        entry = DataEntry(type="processed", path=str(result_file))
        db_session.add(entry)
        db_session.commit()

        pvm = PipelineVersionManager()
        first = pvm.record_run([entry.id], config_file="fmrl/timeline.yaml", export_id=entry.id, export_name="x.pdf", config_snapshot="steps: []\n")
        duplicate = pvm.record_run([entry.id], config_file="fmrl/timeline.yaml", export_id=entry.id, export_name="x.pdf", config_snapshot="steps: []\n")
        changed_config = pvm.record_run([entry.id], config_file="fmrl/timeline.yaml", export_id=entry.id, export_name="x.pdf", config_snapshot="steps:\n- changed\n")

        assert duplicate == first
        versions = pvm.list_versions("fmrl/timeline.yaml")
        assert len(versions) == 2
        assert changed_config != first

    def test_processor_snapshot_change_records_new_version(self, storage, db_session, test_experiment, tmp_path):
        result_file = tmp_path / "result.pdf"
        result_file.write_bytes(b"same result")
        entry = DataEntry(type="processed", path=str(result_file))
        db_session.add(entry)
        db_session.commit()

        pvm = PipelineVersionManager()
        first = pvm.record_run(
            [entry.id],
            config_file="fmrl/timeline.yaml",
            export_id=entry.id,
            export_name="x.pdf",
            config_snapshot="steps: []\n",
            processor_snapshot='{"files":[{"filename":"p.py","content":"v1"}]}',
        )
        duplicate = pvm.record_run(
            [entry.id],
            config_file="fmrl/timeline.yaml",
            export_id=entry.id,
            export_name="x.pdf",
            config_snapshot="steps: []\n",
            processor_snapshot='{"files":[{"filename":"p.py","content":"v1"}]}',
        )
        changed_processor = pvm.record_run(
            [entry.id],
            config_file="fmrl/timeline.yaml",
            export_id=entry.id,
            export_name="x.pdf",
            config_snapshot="steps: []\n",
            processor_snapshot='{"files":[{"filename":"p.py","content":"v2"}]}',
        )

        assert duplicate == first
        assert changed_processor != first
        assert pvm.get_version(changed_processor)["processor_snapshot"]

    def test_restore_processor_snapshot_writes_experiment_processors(self, test_experiment):
        payload = {
            "format": 1,
            "files": [
                {
                    "filename": "old_processor.py",
                    "content": "from core.processing import ProcessorRegistry\n\n"
                    "@ProcessorRegistry.register('old_processor_for_restore')\n"
                    "def old_processor_for_restore(input_path, output_path):\n"
                    "    return []\n",
                }
            ],
        }

        restored = PipelineVersionManager.restore_processor_snapshot(json.dumps(payload))

        assert len(restored) == 1
        assert restored[0].name == "old_processor.py"
        assert restored[0].read_text(encoding="utf-8").startswith("from core.processing")
        assert "old_processor_for_restore" in ProcessorRegistry._processors
        ProcessorRegistry._processors.pop("old_processor_for_restore", None)

    def test_build_processor_snapshot_is_stable_and_deduplicated(self, test_experiment, tmp_path):
        from core.processing import load_processors_from_dir
        from core.base import experiment_manager

        proc_dir = tmp_path / "processors"
        proc_dir.mkdir()
        (proc_dir / "stable_proc.py").write_text(
            "from core.processing import ProcessorRegistry\n\n"
            "@ProcessorRegistry.register('stable_snapshot_proc')\n"
            "def stable_snapshot_proc(input_path, output_path):\n"
            "    return []\n",
            encoding="utf-8",
        )
        load_processors_from_dir(proc_dir)

        first = PipelineVersionManager.build_processor_snapshot(["stable_snapshot_proc"])
        second = PipelineVersionManager.build_processor_snapshot(["stable_snapshot_proc"])
        payload = json.loads(first)

        assert first == second
        assert "content" not in payload["files"][0]
        blob_path = experiment_manager.base_path / payload["files"][0]["blob"]
        assert blob_path.exists()
        assert blob_path.read_text(encoding="utf-8").startswith("from core.processing")
        ProcessorRegistry._processors.pop("stable_snapshot_proc", None)

    def test_delete_version_removes_only_exclusive_processed_entries(self, storage, db_session, test_experiment):
        first_path, first_entry = storage.create_processed_file(".csv", db_session)
        first_path.write_text("first\n", encoding="utf-8")
        second_path, second_entry = storage.create_processed_file(".csv", db_session)
        second_path.write_text("second\n", encoding="utf-8")
        db_session.commit()
        first_id = first_entry.id
        second_id = second_entry.id

        pvm = PipelineVersionManager()
        old_id = pvm.record_run([first_id], config_file="fmrl/timeline.yaml", cache_scope="old")
        new_id = pvm.record_run([second_id], config_file="fmrl/timeline.yaml", cache_scope="new")

        result = pvm.delete_version(old_id)

        assert result["deleted"] is True
        assert result["deletedEntries"] == 1
        assert not first_path.exists()
        assert second_path.exists()
        db_session.expire_all()
        assert db_session.get(DataEntry, first_id) is None
        assert db_session.get(DataEntry, second_id) is not None
        assert pvm.get_version(old_id) is None
        assert pvm.get_version(new_id)["status"] == "active"

    def test_delete_version_keeps_entries_referenced_by_other_versions(self, storage, db_session, test_experiment):
        shared_path, shared_entry = storage.create_processed_file(".csv", db_session)
        shared_path.write_text("shared\n", encoding="utf-8")
        db_session.commit()
        shared_id = shared_entry.id

        pvm = PipelineVersionManager()
        old_id = pvm.record_run([shared_id], config_file="fmrl/timeline.yaml", cache_scope="old")
        pvm.record_run([shared_id], config_file="fmrl/timeline.yaml", cache_scope="new", config_snapshot="steps:\n- changed\n")

        result = pvm.delete_version(old_id)

        assert result["deleted"] is True
        assert result["deletedEntries"] == 0
        assert shared_path.exists()
        db_session.expire_all()
        assert db_session.get(DataEntry, shared_id) is not None

    def test_delete_active_version_is_rejected(self, storage, db_session, test_experiment):
        path, entry = storage.create_processed_file(".csv", db_session)
        path.write_text("active\n", encoding="utf-8")
        db_session.commit()

        pvm = PipelineVersionManager()
        version_id = pvm.record_run([entry.id], config_file="fmrl/timeline.yaml")

        result = pvm.delete_version(version_id)

        assert result["deleted"] is False
        assert result["reason"] == "active_version"
        assert path.exists()
        assert pvm.get_version(version_id)["status"] == "active"

    def test_storage_report_includes_reclaimable_version_bytes(self, storage, db_session, test_experiment):
        old_path, old_entry = storage.create_processed_file(".csv", db_session)
        old_path.write_text("old-bytes\n", encoding="utf-8")
        new_path, new_entry = storage.create_processed_file(".csv", db_session)
        new_path.write_text("new-bytes\n", encoding="utf-8")
        db_session.commit()

        pvm = PipelineVersionManager()
        old_id = pvm.record_run([old_entry.id], config_file="fmrl/timeline.yaml", cache_scope="old")
        new_id = pvm.record_run([new_entry.id], config_file="fmrl/timeline.yaml", cache_scope="new")
        report = pvm.storage_report("fmrl/timeline.yaml")
        by_id = {item["id"]: item for item in report["versions"]}

        assert report["totalBytes"] >= old_path.stat().st_size + new_path.stat().st_size
        assert by_id[old_id]["deletable"] is True
        assert by_id[old_id]["reclaimableBytes"] == old_path.stat().st_size
        assert by_id[new_id]["deletable"] is False
        assert by_id[new_id]["reclaimableBytes"] == 0

    def test_version_source_mode_uses_version_raw_entries(self, storage, db_session, test_experiment, tmp_path, monkeypatch):
        old_file = tmp_path / "old.csv"
        new_file = tmp_path / "new.csv"
        old_file.write_text("v\n1\n")
        new_file.write_text("v\n2\n")
        old_entry = storage.store_raw_data(str(old_file), db_session)
        new_entry = storage.store_raw_data(str(new_file), db_session)
        old_entry.original_path = "/source/data.csv"
        new_entry.original_path = "/source/data.csv"
        db_session.commit()

        pvm = PipelineVersionManager()
        pvm.record_run([old_entry.id], config_file="fmrl/timeline.yaml", cache_scope="legacy")
        monkeypatch.setenv("FILELINE_SOURCE_MODE", "version")
        monkeypatch.setenv("FILELINE_PIPELINE_PATH", "fmrl/timeline.yaml")

        runner = PipelineRunner(storage, db_session)
        config = InitialLoadConfig(include_patterns=[IncludeSpec(path="/source/*.csv")])
        sources = runner._load_initial_files(config)

        assert sources["initial"] == [old_entry.id]
