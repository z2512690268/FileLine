"""pytest 共享 fixtures"""
import sys
import os
import shutil
import tempfile
from pathlib import Path
from datetime import datetime

import pytest

# 确保项目根在 sys.path
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from core.base import experiment_manager, get_session, init_db
from core.storage import FileStorage
from core.processing import ProcessorRegistry, DataProcessor
from core.models import Base


@pytest.fixture(scope="function")
def test_experiment():
    """创建/清理一个独立测试实验"""
    name = f"_test_{datetime.now().strftime('%H%M%S%f')}"
    experiment_manager.create(name, "pytest temp")
    old_current = experiment_manager.current_experiment
    experiment_manager.set_current(name)
    init_db()
    yield name
    # 清理
    experiment_manager.set_current(old_current) if old_current else None
    exps = experiment_manager.get_experiments()
    exp_dir = experiment_manager._PROJECT_ROOT / "experiments" / name
    if name in exps:
        del exps[name]
        experiment_manager._save_experiments(exps)
    if exp_dir.exists():
        shutil.rmtree(exp_dir)


@pytest.fixture(scope="function")
def db_session(test_experiment):
    """每个测试一个独立 DB session"""
    from core.base import get_session
    with get_session() as session:
        yield session


@pytest.fixture(scope="function")
def storage(test_experiment):
    """测试用的 FileStorage"""
    return FileStorage()


@pytest.fixture(scope="function")
def processor():
    """注册一个测试用单输出 processor"""
    name = "_test_p_single"

    @ProcessorRegistry.register(name, input_type="single", output_type="single", output_ext=".csv")
    def _single_processor(input_path, output_path, multiplier=2):
        import pandas as pd
        df = pd.read_csv(input_path.path)
        df["value"] = df["value"] * multiplier
        df.to_csv(output_path, index=False)
        return ["processed"]

    yield name
    ProcessorRegistry._processors.pop(name, None)


@pytest.fixture(scope="function")
def multi_processor():
    """注册一个测试用多输出 processor"""
    name = "_test_p_multi"

    @ProcessorRegistry.register(name, input_type="single", output_type="multi", output_ext="")
    def _multi_processor(input_path, output_dir, split_col="cat"):
        import pandas as pd
        df = pd.read_csv(input_path.path)
        results = []
        for val in df[split_col].unique():
            fname = f"{val}.csv"
            df[df[split_col] == val].to_csv(output_dir / fname, index=False)
            results.append((fname, f"val:{val}"))
        return results

    yield name
    ProcessorRegistry._processors.pop(name, None)


@pytest.fixture(scope="function")
def sample_csv(tmp_path):
    """创建一个测试用 CSV 文件"""
    path = tmp_path / "test_data.csv"
    path.write_text("cat,value\nA,10\nB,20\nA,30\nC,40\n")
    return str(path)


@pytest.fixture(scope="function")
def sample_csv_small(tmp_path):
    """另一个测试用 CSV"""
    path = tmp_path / "small.csv"
    path.write_text("x,y\n1,2\n3,4\n")
    return str(path)
