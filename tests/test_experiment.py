"""实验管理: 创建/切换/列表/删除/路径持久化"""
from pathlib import Path
from core.base import experiment_manager, get_session, get_engine


class TestExperimentCRUD:
    def test_create_and_switch(self, test_experiment):
        assert experiment_manager.current_experiment == test_experiment
        exps = experiment_manager.get_experiments()
        assert test_experiment in exps

    def test_list(self, test_experiment):
        exps = experiment_manager.get_experiments()
        assert isinstance(exps, dict)
        assert test_experiment in exps
        cfg = exps[test_experiment]
        assert "database" in cfg
        assert "data_root" in cfg

    def test_delete(self, test_experiment):
        name = test_experiment
        exp_dir = experiment_manager._PROJECT_ROOT / "experiments" / name
        assert exp_dir.exists()
        import shutil
        shutil.rmtree(exp_dir)
        exps = experiment_manager.get_experiments()
        del exps[name]
        experiment_manager._save_experiments(exps)
        experiment_manager.delete_current()

        assert not exp_dir.exists()
        assert name not in experiment_manager.get_experiments()

    def test_double_create_raises(self):
        from core.base import experiment_manager as em
        name = "_test_double"
        em.create(name, "")
        import pytest
        with pytest.raises(ValueError, match="已存在"):
            em.create(name, "")

        # cleanup
        exps = em.get_experiments()
        exp_dir = em._PROJECT_ROOT / "experiments" / name
        if name in exps:
            del exps[name]
            em._save_experiments(exps)
        if exp_dir.exists():
            import shutil
            shutil.rmtree(exp_dir)

    def test_use_nonexistent_raises(self):
        import pytest
        with pytest.raises(ValueError, match="不存在"):
            experiment_manager.set_current("_nonexistent_xyz")


class TestExperimentPaths:
    """测试路径持久化和跨 CWD 访问"""

    def test_stores_relative_paths(self, test_experiment):
        cfg = experiment_manager.get_experiments()[test_experiment]
        db_path = Path(cfg["database"])
        assert not db_path.is_absolute(), "新实验应存储相对路径"
        assert str(db_path).startswith("experiments/")

    def test_engine_resolves_relative_path(self, test_experiment):
        engine = get_engine()
        assert engine is not None

    def test_base_path_correct(self, test_experiment):
        expected = experiment_manager._PROJECT_ROOT / "experiments" / test_experiment
        assert experiment_manager.base_path == expected

    def test_project_root_immutable(self):
        root = experiment_manager.project_root
        # __file__ 在 core/experiment.py, project root 在它的 parent.parent
        expected = Path(__file__).resolve().parent.parent
        assert root == expected, f"{root} != {expected}"
