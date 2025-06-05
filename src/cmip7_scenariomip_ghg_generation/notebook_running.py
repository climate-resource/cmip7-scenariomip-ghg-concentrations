"""
Notebook running functionality
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import jupytext
import papermill


def run_notebook(notebook: Path, run_notebooks_dir: Path, parameters: dict[str, Any], identity: str) -> None:
    """
    Run a notebook

    Takes the raw notebook, makes a direct copy
    and an executed copy using `identity` to ensure that multiple runs
    with different parameters don't overwrite each other.

    Parameters
    ----------
    notebook
        Path to the raw notebook to run (expected to be a `.py` file)

    run_notebooks_dir
        Directory in which to write out the run/executed notebooks

    parameters
        Parameters to pass to the notebook

    identity
        Identity to use when creating the output notebook name
    """
    notebook_jupytext = jupytext.read(notebook)

    # Write the .py file as .ipynb
    in_notebook = run_notebooks_dir / f"{notebook.stem}_{identity}_unexecuted.ipynb"
    in_notebook.parent.mkdir(exist_ok=True, parents=True)
    jupytext.write(notebook_jupytext, in_notebook, fmt="ipynb")

    output_notebook = run_notebooks_dir / f"{notebook.stem}_{identity}.ipynb"
    output_notebook.parent.mkdir(exist_ok=True, parents=True)

    print(f"Executing {notebook.name=} with {parameters=} from {in_notebook=}. " f"Writing to {output_notebook=}")
    # Execute to specific directory
    papermill.execute_notebook(in_notebook, output_notebook, parameters=parameters)
