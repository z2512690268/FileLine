"""app_utils: EntryInfo/数据查询/血缘追溯"""
from pathlib import Path
from core.base import get_session
from core.models import DataEntry
from core.storage import FileStorage


class TestGetDataEntries:
    def test_list_all(self, test_experiment, sample_csv):
        storage = FileStorage()
        with get_session() as session:
            storage.store_raw_data(sample_csv, session)
            session.commit()

        from app_utils import get_data_entries
        entries = get_data_entries(limit=10)
        assert len(entries) >= 1
        e = entries[0]
        assert e.id is not None
        assert e.type is not None
        assert e.path is not None

    def test_filter_by_type(self, test_experiment, sample_csv):
        storage = FileStorage()
        with get_session() as session:
            storage.store_raw_data(sample_csv, session)
            session.commit()

        from app_utils import get_data_entries
        entries = get_data_entries(entry_type="raw")
        for e in entries:
            assert e.type == "raw"

    def test_filter_by_tag(self, test_experiment, sample_csv):
        storage = FileStorage()
        with get_session() as session:
            entry = storage.store_raw_data(sample_csv, session)
            from core.models import Tag
            tag = session.query(Tag).filter_by(name="test_filter").first()
            if not tag:
                tag = Tag(name="test_filter")
                session.add(tag)
            entry.tags.append(tag)
            session.commit()

        from app_utils import get_data_entries
        entries = get_data_entries(tags=["test_filter"])
        assert len(entries) >= 1
        assert "test_filter" in entries[0].tags


class TestProvenance:
    def test_build_tree(self, test_experiment, sample_csv):
        from core.processing import ProcessorRegistry, DataProcessor
        from app_utils import build_provenance_tree

        # Register processor
        @ProcessorRegistry.register("_test_prov_proc", input_type="single", output_ext=".csv")
        def _prov_proc(inp, output_path):
            import pandas as pd
            pd.read_csv(inp.path).to_csv(output_path, index=False)
            return "prov"

        storage = FileStorage()
        with get_session() as session:
            raw = storage.store_raw_data(sample_csv, session)
            session.commit()
            dp = DataProcessor(storage, session)
            processed = dp.run("_test_prov_proc", raw.id)
            session.commit()
            processed_id = processed.id
            raw_id = raw.id

        tree = build_provenance_tree(processed_id)
        assert tree is not None
        assert tree.entry_id == processed_id
        assert len(tree.children) > 0
        assert tree.children[0].entry_id == raw_id
        assert tree is not None
        assert tree.entry_id == processed.id
        # 应该有父节点
        assert len(tree.children) > 0
        assert tree.children[0].entry_id == raw.id

        ProcessorRegistry._processors.pop("_test_prov_proc", None)

    def test_tree_for_nonexistent_returns_none(self):
        from app_utils import build_provenance_tree
        tree = build_provenance_tree(99999)
        assert tree is None


class TestEntryDataFrame:
    def test_csv(self, test_experiment, sample_csv):
        from app_utils import get_entry_dataframe
        storage = FileStorage()
        with get_session() as session:
            entry = storage.store_raw_data(sample_csv, session)
            session.commit()
            entry_id = entry.id

        df = get_entry_dataframe(entry_id)
        assert df is not None
        assert list(df.columns) == ["cat", "value"]
        assert len(df) == 4

    def test_missing_file_returns_none(self, test_experiment):
        from app_utils import get_entry_dataframe
        df = get_entry_dataframe(99999)
        assert df is None


class TestFormatFileSize:
    def test_format(self):
        from app_utils import format_file_size
        # 用当前文件测试
        path = __file__
        result = format_file_size(path)
        assert result is not None
        assert "B" in result or "KB" in result

    def test_missing_file(self):
        from app_utils import format_file_size
        result = format_file_size("/nonexistent/file.xyz")
        assert result == "?"
