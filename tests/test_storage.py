"""FileStorage: 文件存储/ID 命名/导出元数据"""
from pathlib import Path
from core.storage import FileStorage
from core.models import DataEntry


class TestStoreRawData:
    def test_copies_file_and_creates_entry(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        assert entry.id is not None
        assert entry.path is not None
        target = Path(entry.path)
        assert target.exists()
        assert target.suffix == ".csv"
        # 文件名 = ID
        assert target.stem == str(entry.id)
        # 文件内容一致
        assert target.read_text() == Path(sample_csv).read_text()
        # 原始路径已记录
        assert entry.original_path == str(Path(sample_csv).absolute())

    def test_id_naming(self, storage, sample_csv_small, db_session):
        e1 = storage.store_raw_data(sample_csv_small, db_session)
        e2 = storage.store_raw_data(sample_csv_small, db_session)
        e3 = storage.store_raw_data(sample_csv_small, db_session)
        ids = [e1.id, e2.id, e3.id]
        assert len(set(ids)) == 3, "每个文件应有唯一 ID"
        for e in (e1, e2, e3):
            assert Path(e.path).stem == str(e.id)

    def test_entry_type_is_raw(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        assert entry.type == "raw"

    def test_directory_structure(self, storage, sample_csv, db_session):
        storage.store_raw_data(sample_csv, db_session)
        base = storage.base_path
        assert (base / "raw").is_dir()
        assert (base / "processed").is_dir()
        assert (base / "exports").is_dir()


class TestCreateProcessedFile:
    def test_creates_entry_and_path(self, storage, db_session):
        ext = ".parquet"
        path, entry = storage.create_processed_file(ext=ext, session=db_session)
        assert entry.id is not None
        assert entry.path == str(path)
        assert path.suffix == ext
        assert path.stem == str(entry.id)

    def test_type_is_processed(self, storage, db_session):
        _, entry = storage.create_processed_file(".csv", session=db_session)
        assert entry.type == "processed"

    def test_default_extension(self, storage, db_session):
        path, entry = storage.create_processed_file(session=db_session)
        assert path.suffix == ".csv"

    def test_multiple_files(self, storage, db_session):
        _, e1 = storage.create_processed_file(".csv", session=db_session)
        _, e2 = storage.create_processed_file(".txt", session=db_session)
        _, e3 = storage.create_processed_file(".parquet", session=db_session)
        assert e1.id < e2.id < e3.id, "ID 应自增"


class TestExport:
    def test_create_export_updates_meta(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        name = "test_export.csv"
        path = storage.create_export_file(name, entry.id)
        # create_export_file 仅创建 meta + 确保目录存在，不写入文件内容
        assert path.parent.exists()
        meta = storage._meta_cache.get(name)
        assert meta is not None
        assert meta["id"] == entry.id

    def test_exports_meta_persistence(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        storage.create_export_file("persist_test.csv", entry.id)
        # 新建一个 storage 实例应能加载之前保存的 meta
        storage2 = FileStorage()
        assert "persist_test.csv" in storage2._meta_cache
        assert storage2._meta_cache["persist_test.csv"]["id"] == entry.id

    def test_nested_export(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        path = storage.create_export_file("subdir/nested.csv", entry.id)
        assert path.parent.exists()
        assert path.parent.name == "subdir"
