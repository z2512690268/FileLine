import shutil
from pathlib import Path

from core.processing import InputPath, ProcessorRegistry


@ProcessorRegistry.register(
    name="file_identity",
    input_type="single",
    output_ext=ProcessorRegistry.SAME_OUTPUT_EXT,
)
def file_identity(input_path: InputPath, output_path: Path):
    """Copy the input file unchanged.

    This is useful as an editable placeholder step in a DAG: inserting it keeps
    the pipeline result stable while giving the user a real step to replace.
    """
    shutil.copyfile(input_path.path, output_path)
    return ["identity"]
