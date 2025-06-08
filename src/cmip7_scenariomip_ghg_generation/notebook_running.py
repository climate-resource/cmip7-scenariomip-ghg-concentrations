"""
Notebook running functionality
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

import jupytext
import papermill


def run_notebook(  # noqa: PLR0913
    notebook: Path,
    run_notebooks_dir: Path,
    parameters: dict[str, Any],
    identity: str,
    verbose: bool | int = False,
    progress: bool = False,
) -> None:
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

    verbose
        Should a message about the notebook being run be printed?

        If an integer, this sets the level of verbosity
        (higher is more verbose).

    progress
        Should the progress of the execution be shown?
    """
    if isinstance(verbose, bool):
        verbosity_i = int(verbose)
    else:
        verbosity_i = verbose

    notebook_jupytext = jupytext.read(notebook)

    # Write the .py file as .ipynb
    in_notebook = run_notebooks_dir / f"{notebook.stem}_{identity}_unexecuted.ipynb"
    in_notebook.parent.mkdir(exist_ok=True, parents=True)
    jupytext.write(notebook_jupytext, in_notebook, fmt="ipynb")

    output_notebook = run_notebooks_dir / f"{notebook.stem}_{identity}.ipynb"
    output_notebook.parent.mkdir(exist_ok=True, parents=True)

    if verbosity_i == 1:
        print(
            f"Executing, in {os.getpid()=} and {threading.get_ident()=}, {notebook.name=}\n"
            f"Writing to {output_notebook=}. "
        )

    elif verbosity_i == 2:  # noqa: PLR2004
        print(
            f"Executing, in {os.getpid()=}, "
            f"{notebook.name=}\nwith {parameters=}\nfrom {in_notebook=}.\n"
            f"Writing to {output_notebook=}. "
        )

    # Execute
    papermill.execute_notebook(in_notebook, output_notebook, parameters=parameters, progress_bar=progress)
