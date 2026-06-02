"""Processor 注册/单输出/多输出/标签/输入类型"""
import pandas as pd
from pathlib import Path
from core.processing import ProcessorRegistry, DataProcessor


class TestRegistry:
    def test_register_and_get(self):
        @ProcessorRegistry.register("_test_reg", input_type="single", output_type="single", output_ext=".csv")
        def dummy(inp, output_path, **kw):
            pass

        info = ProcessorRegistry.get_processor("_test_reg")
        assert info["input_type"] == "single"
        assert info["output_type"] == "single"
        assert info["output_ext"] == ".csv"
        assert "hash" in info
        ProcessorRegistry._processors.pop("_test_reg", None)

    def test_register_duplicate_raises(self):
        import pytest
        name = "_test_dup"

        @ProcessorRegistry.register(name, input_type="single", output_ext=".txt")
        def a(inp, output_path, **kw): pass

        with pytest.raises(ValueError, match="已注册"):
            @ProcessorRegistry.register(name, input_type="single", output_ext=".txt")
            def b(inp, output_path, **kw): pass

        ProcessorRegistry._processors.pop(name, None)

    def test_invalid_input_type_raises(self):
        import pytest
        with pytest.raises(ValueError):
            ProcessorRegistry.register("x", input_type="invalid")

    def test_invalid_output_type_raises(self):
        import pytest
        with pytest.raises(ValueError):
            ProcessorRegistry.register("x", output_type="invalid")

    def test_output_ext_auto_dot(self):
        name = "_test_ext"

        @ProcessorRegistry.register(name, output_ext="pdf")
        def d(inp, output_path, **kw): pass

        assert ProcessorRegistry.get_processor(name)["output_ext"] == ".pdf"
        ProcessorRegistry._processors.pop(name, None)


class TestSingleOutput:
    """单输出 processor: single/multi/none 输入"""

    @ProcessorRegistry.register("_test_adder", input_type="single", output_ext=".csv")
    def _adder(input_path, output_path, add_val=10):
        df = pd.read_csv(input_path.path)
        df["value"] = df["value"] + add_val
        df.to_csv(output_path, index=False)
        return "added"

    def test_single_input(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        db_session.commit()
        dp = DataProcessor(storage, db_session)
        result = dp.run("_test_adder", entry.id, add_val=5)
        assert isinstance(result, Path) is False  # 不是 old style
        assert result.id is not None
        assert "added" in [t.name for t in result.tags]
        # 验证内容
        out_df = pd.read_csv(result.path)
        assert out_df["value"].tolist() == [15, 25, 35, 45]
        assert len(result.parents) == 1
        assert result.parents[0].id == entry.id

    def test_multi_input(self, storage, sample_csv, sample_csv_small, db_session):
        @ProcessorRegistry.register("_test_csv_merge", input_type="multi", output_ext=".csv")
        def _merger(input_paths, output_path, **kw):
            dfs = [pd.read_csv(p.path) for p in input_paths]
            pd.concat(dfs).to_csv(output_path, index=False)
            return ["merged"]

        e1 = storage.store_raw_data(sample_csv, db_session)
        e2 = storage.store_raw_data(sample_csv_small, db_session)
        db_session.commit()
        dp = DataProcessor(storage, db_session)
        result = dp.run("_test_csv_merge", [e1.id, e2.id])
        assert "merged" in [t.name for t in result.tags]
        assert len(result.parents) == 2
        ProcessorRegistry._processors.pop("_test_csv_merge", None)

    def test_none_input(self, storage, db_session):
        """零输入 processor"""
        @ProcessorRegistry.register("_test_gen", input_type="none", output_ext=".txt")
        def _gen(output_path, lines=3):
            with open(output_path, "w") as f:
                for i in range(lines):
                    f.write(f"line{i}\n")
            return "generated"

        dp = DataProcessor(storage, db_session)
        result = dp.run("_test_gen", None, lines=3)
        assert result.type == "processed"
        assert "generated" in [t.name for t in result.tags]
        lines = Path(result.path).read_text().strip().split("\n")
        assert len(lines) == 3
        ProcessorRegistry._processors.pop("_test_gen", None)

    def test_tag_variants(self, storage, sample_csv, db_session):
        """processor 返回 str / list / None 都正常"""
        @ProcessorRegistry.register("_test_tag_ret", input_type="single", output_ext=".csv")
        def _tag_ret(input_path, output_path, mode="str"):
            df = pd.read_csv(input_path.path)
            df.to_csv(output_path, index=False)
            if mode == "str":
                return "single_tag"
            elif mode == "list":
                return ["tag1", "tag2"]
            else:
                return None

        entry = storage.store_raw_data(sample_csv, db_session)
        db_session.commit()
        dp = DataProcessor(storage, db_session)

        for mode, expected in [("str", ["single_tag"]), ("list", ["tag1", "tag2"]), ("none", [])]:
            ProcessorRegistry._processors["_test_tag_ret"]["hash"] = "r" + mode
            result = dp.run("_test_tag_ret", entry.id, mode=mode)
            assert sorted([t.name for t in result.tags]) == sorted(expected), f"mode={mode}"

        ProcessorRegistry._processors.pop("_test_tag_ret", None)


class TestMultiOutput:
    """多输出 processor: 按列拆分"""

    @ProcessorRegistry.register("_test_split", input_type="single", output_type="multi", output_ext="")
    def _split(input_path, output_dir, split_col="cat"):
        df = pd.read_csv(input_path.path)
        for val in df[split_col].unique():
            fname = f"{val}.csv"
            df[df[split_col] == val].to_csv(output_dir / fname, index=False)
            yield (fname, f"val:{val}")

    def test_returns_list(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        db_session.commit()
        dp = DataProcessor(storage, db_session)
        result = dp.run("_test_split", entry.id, split_col="cat")
        assert isinstance(result, list)
        assert len(result) == 3  # A, B, C

    def test_each_output_has_id_named_file(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        db_session.commit()
        dp = DataProcessor(storage, db_session)
        results = dp.run("_test_split", entry.id)
        for e in results:
            assert Path(e.path).stem.startswith(str(e.id))
            assert Path(e.path).exists()

    def test_tags_and_parents(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        db_session.commit()
        dp = DataProcessor(storage, db_session)
        results = dp.run("_test_split", entry.id)
        tag_names = set()
        for e in results:
            tag_names.update(t.name for t in e.tags)
            assert len(e.parents) == 1
            assert e.parents[0].id == entry.id
        # A, B, C 三个值各有标签
        assert any("val:A" in t for t in tag_names)
        assert any("val:B" in t for t in tag_names)
        assert any("val:C" in t for t in tag_names)
        assert len(results) == 3

    def test_file_content_correct(self, storage, sample_csv, db_session):
        entry = storage.store_raw_data(sample_csv, db_session)
        db_session.commit()
        dp = DataProcessor(storage, db_session)
        results = dp.run("_test_split", entry.id)
        for e in results:
            df = pd.read_csv(e.path)
            # 来自同一分类的值一致
            val = df["cat"].iloc[0]
            assert (df["cat"] == val).all()

    def test_multi_with_none_input(self, storage, db_session):
        """零输入 + 多输出"""
        @ProcessorRegistry.register("_test_gen_multi", input_type="none", output_type="multi", output_ext="")
        def _gen(output_dir, n=2):
            import pandas as pd
            for i in range(n):
                fname = f"out_{i}.csv"
                pd.DataFrame({"x": [i]}).to_csv(output_dir / fname, index=False)
                yield (fname, f"idx:{i}")

        dp = DataProcessor(storage, db_session)
        results = dp.run("_test_gen_multi", None, n=3)
        assert isinstance(results, list)
        assert len(results) == 3
        for e in results:
            assert Path(e.path).exists()
            assert Path(e.path).stem.startswith(str(e.id))
        ProcessorRegistry._processors.pop("_test_gen_multi", None)
