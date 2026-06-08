"""CLI command smoke tests."""
import json
import re

from click.testing import CliRunner

from core.base import get_session
from core.models import DataEntry, PipelineVersion


runner = CliRunner()


class TestDataCommands:
    """python main.py data ..."""

    def test_add(self, test_experiment, tmp_path):
        from commands.data_commands.add import add_cmd

        f = tmp_path / "input.csv"
        f.write_text("a,b\n1,2\n")
        result = runner.invoke(add_cmd, [str(f)])
        assert result.exit_code == 0
        assert re.search(r"ID:\s*(\d+)", result.output), result.output

    def test_show(self, test_experiment, tmp_path):
        from commands.data_commands.add import add_cmd
        from commands.data_commands.show import show_cmd

        f = tmp_path / "show.csv"
        f.write_text("x,10\n")
        runner.invoke(add_cmd, [str(f)])
        result = runner.invoke(show_cmd, ["--limit", "5"])
        assert result.exit_code == 0
        assert "show.csv" in result.output or re.search(r"\bID\b|\d+", result.output)

    def test_show_filter_by_type(self, test_experiment):
        from commands.data_commands.show import show_cmd

        result = runner.invoke(show_cmd, ["--type", "raw", "--limit", "5"])
        assert result.exit_code == 0

    def test_delete(self, test_experiment, tmp_path):
        from commands.data_commands.add import add_cmd
        from commands.data_commands.delete import delete_cmd

        f = tmp_path / "del.csv"
        f.write_text("v,1\n")
        r = runner.invoke(add_cmd, [str(f)])
        m = re.search(r"ID:\s*(\d+)", r.output)
        assert m
        eid = m.group(1)

        result = runner.invoke(delete_cmd, [eid, "-y"])
        assert result.exit_code == 0
        assert "deleted 1 data entry" in result.output
        with get_session() as session:
            assert session.get(DataEntry, int(eid)) is None

    def test_delete_referenced_entry_requires_strong_confirmation(self, test_experiment, tmp_path):
        from commands.data_commands.add import add_cmd
        from commands.data_commands.delete import delete_cmd

        f = tmp_path / "referenced.csv"
        f.write_text("v,1\n")
        r = runner.invoke(add_cmd, [str(f)])
        m = re.search(r"ID:\s*(\d+)", r.output)
        assert m
        eid = int(m.group(1))
        with get_session() as session:
            session.add(PipelineVersion(config_file="demo.yaml", entry_ids=json.dumps([eid]), status="active"))
            session.commit()

        result = runner.invoke(delete_cmd, [str(eid), "-y"])
        assert result.exit_code != 0
        assert "Refusing unsafe delete" in result.output
        with get_session() as session:
            assert session.get(DataEntry, eid) is not None

    def test_delete_referenced_entry_with_exact_confirmation(self, test_experiment, tmp_path):
        from commands.data_commands.add import add_cmd
        from commands.data_commands.delete import delete_cmd

        f = tmp_path / "force_referenced.csv"
        f.write_text("v,1\n")
        r = runner.invoke(add_cmd, [str(f)])
        m = re.search(r"ID:\s*(\d+)", r.output)
        assert m
        eid = int(m.group(1))
        with get_session() as session:
            session.add(PipelineVersion(config_file="demo.yaml", entry_ids=json.dumps([eid]), status="superseded"))
            session.commit()

        result = runner.invoke(delete_cmd, [
            str(eid),
            "-y",
            "--force-referenced",
            "--confirm-referenced",
            "DELETE REFERENCED DATA",
        ])
        assert result.exit_code == 0
        assert "deleted 1 data entry" in result.output
        with get_session() as session:
            assert session.get(DataEntry, eid) is None

    def test_delete_nonexistent(self, test_experiment):
        from commands.data_commands.delete import delete_cmd

        result = runner.invoke(delete_cmd, ["99999", "-y"])
        assert result.exit_code == 0

    def test_check(self, test_experiment):
        from commands.data_commands.check import check_cmd

        result = runner.invoke(check_cmd)
        assert result.exit_code == 0

    def test_check_with_fix_does_not_crash(self, test_experiment):
        from commands.data_commands.check import check_cmd

        result = runner.invoke(check_cmd, ["--fix"])
        assert result.exit_code == 0

    def test_list_recent(self, test_experiment):
        from commands.data_commands.list_recent import list_recent_cmd

        result = runner.invoke(list_recent_cmd, ["--limit", "3"])
        assert result.exit_code == 0

    def test_tag(self, test_experiment, tmp_path):
        from commands.data_commands.add import add_cmd
        from commands.data_commands.tag import tag_cmd

        f = tmp_path / "tag.csv"
        f.write_text("v,1\n")
        r = runner.invoke(add_cmd, [str(f)])
        m = re.search(r"ID:\s*(\d+)", r.output)
        assert m
        eid = m.group(1)

        result = runner.invoke(tag_cmd, [eid, "mytag"])
        assert result.exit_code == 0
        with get_session() as session:
            entry = session.get(DataEntry, int(eid))
            assert "mytag" in [tag.name for tag in entry.tags]


class TestExperimentCommands:
    """python main.py experiment ..."""

    def test_list(self, test_experiment):
        from commands.experiment_commands import list

        result = runner.invoke(list)
        assert result.exit_code == 0
        assert test_experiment in result.output

    def test_use(self):
        from commands.experiment_commands import use  # noqa: F401

        # The fixture already switches to the test experiment.
        pass

    def test_create_and_delete(self):
        from commands.experiment_commands import create, delete

        name = "_test_cli_exp"
        r = runner.invoke(create, [name, "--description", "cli test"])
        assert r.exit_code == 0
        r2 = runner.invoke(delete, [name])
        assert r2.exit_code == 0


class TestProcessCommand:
    def test_run_single_output(self, test_experiment, tmp_path):
        from commands.data_commands.add import add_cmd
        from commands.process_commands import run
        from core.processing import ProcessorRegistry

        f = tmp_path / "proc.csv"
        f.write_text("value,10\n")

        @ProcessorRegistry.register("_cli_test_proc", input_type="single", output_ext=".csv")
        def _cli_double(inp, output_path):
            import pandas as pd

            df = pd.read_csv(inp.path)
            df["value"] = df["value"] * 2
            df.to_csv(output_path, index=False)
            return "doubled"

        r = runner.invoke(add_cmd, [str(f)])
        m = re.search(r"ID:\s*(\d+)", r.output)
        assert m
        eid = m.group(1)
        result = runner.invoke(run, ["_cli_test_proc", eid])
        assert result.exit_code == 0
        assert re.search(r"\d+", result.output)
        ProcessorRegistry._processors.pop("_cli_test_proc", None)

    def test_run_multi_output(self, test_experiment, tmp_path):
        from commands.data_commands.add import add_cmd
        from commands.process_commands import run
        from core.processing import ProcessorRegistry

        f = tmp_path / "multi_cli.csv"
        f.write_text("cat,v\nX,1\nY,2\n")

        @ProcessorRegistry.register("_cli_multi_proc", input_type="single", output_type="multi", output_ext="")
        def _cli_split(inp, output_dir):
            import pandas as pd

            df = pd.read_csv(inp.path)
            for val in df["cat"].unique():
                fname = f"{val}.csv"
                df[df["cat"] == val].to_csv(output_dir / fname, index=False)
                yield (fname,)

        r = runner.invoke(add_cmd, [str(f)])
        m = re.search(r"ID:\s*(\d+)", r.output)
        assert m
        eid = m.group(1)
        result = runner.invoke(run, ["_cli_multi_proc", eid])
        assert result.exit_code == 0
        ids = re.findall(r"\d+", result.output)
        assert len(ids) >= 2, result.output
        ProcessorRegistry._processors.pop("_cli_multi_proc", None)
