"""
Create a bundle ready to upload to Zenodo
"""

from __future__ import annotations

import shutil
import sys
import tarfile
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Annotated

import netCDF4
import tqdm.auto
import typer
from attrs import define
from dotenv import load_dotenv
from loguru import logger

# ruff: noqa: D101, D102, D103


def tar_filter(
    tarinfo: tarfile.TarInfo,
    filters: list[Callable[[Path], bool]],
) -> tarfile.TarInfo | None:
    if any(filter(Path(tarinfo.name)) for filter in filters):
        print(f"    - Not bundling {tarinfo.name}")
        return None

    return tarinfo


def create_tar_file(
    original_bundle_path: Path,
    current_level_path_rel_to_original_bundle_path: Path,
    zenodo_bundle_path: Path,
    filters: list[Callable[[Path], bool]],
    files_only: bool = False,
) -> Path:
    tar_id = "--".join(current_level_path_rel_to_original_bundle_path.parts)
    tar_path = zenodo_bundle_path / f"{tar_id}.tar.gz"
    print(f"Writing to {tar_path}")
    with tarfile.open(tar_path, "w:gz") as tar:
        for file_to_keep_candidate in (original_bundle_path / current_level_path_rel_to_original_bundle_path).iterdir():
            if files_only and not file_to_keep_candidate.is_file():
                continue

            if any(filter(file_to_keep_candidate) for filter in filters):
                print(f"    - Not bundling {file_to_keep_candidate}")
                continue

            print(f"    - Adding {file_to_keep_candidate}")
            tar.add(
                file_to_keep_candidate,
                arcname=file_to_keep_candidate.relative_to(original_bundle_path),
                recursive=True,
                filter=partial(tar_filter, filters=filters),
            )

    return tar_path.relative_to(zenodo_bundle_path)


def create_level_aware_tar(  # noqa: PLR0913
    original_bundle_path: Path,
    current_level_path_rel_to_original_bundle_path: Path,
    zenodo_bundle_path: Path,
    bundle_at_level: int,
    filters: list[Callable[[Path], bool]],
    current_level: int,
) -> list[Path]:
    if bundle_at_level == current_level:
        tar_path = create_tar_file(
            original_bundle_path=original_bundle_path,
            current_level_path_rel_to_original_bundle_path=current_level_path_rel_to_original_bundle_path,
            zenodo_bundle_path=zenodo_bundle_path,
            filters=filters,
        )

        return [tar_path]

    tar_paths = []
    level_files = []
    for level_component in (original_bundle_path / current_level_path_rel_to_original_bundle_path).iterdir():
        if any(filter(level_component) for filter in filters):
            print(f"    - Not bundling {level_component}")
            continue

        if level_component.is_dir():
            tar_path = create_level_aware_tar(
                original_bundle_path=original_bundle_path,
                current_level_path_rel_to_original_bundle_path=level_component.relative_to(original_bundle_path),
                zenodo_bundle_path=zenodo_bundle_path,
                current_level=current_level + 1,
                bundle_at_level=bundle_at_level,
                filters=filters,
            )
            tar_paths.extend(tar_path)

        else:
            level_files.append(level_component)

    if level_files:
        tar_path = create_tar_file(
            original_bundle_path=original_bundle_path,
            current_level_path_rel_to_original_bundle_path=current_level_path_rel_to_original_bundle_path,
            zenodo_bundle_path=zenodo_bundle_path,
            filters=filters,
            files_only=True,
        )
        tar_paths.append(tar_path)

    return tar_paths


@define
class DirectoryBundlingSpecs:
    dir: Path
    """Directory to bundle"""

    level: int
    """Level at which to bundle this directory

    For example, `level=1` means that each sub-directory
    of `dir` will be bundled separately.
    `level=0` means all `dir` will be bundled in a single file.
    `level=2` means each sub-sub-directory of `dir` will be bundled separately.
    """

    filters: list[Callable[[Path], bool]]
    """
    Filters to apply when bundling

    If any filter runs `True`, then the file/directory will not be bundled
    """


def create_zenodo_bundle(zenodo_bundle_path: Path, original_bundle_path: Path) -> tuple[Path, ...]:
    bundled_files = []

    files_to_bundle_at_root_level = (
        original_bundle_path / "Makefile",
        # original_bundle_path / "README.md",
        original_bundle_path / "pixi.lock",
        original_bundle_path / "pyproject.toml",
        original_bundle_path / "zenodo.json",
        # *original_bundle_path.glob("*.yaml"),
    )

    for file in files_to_bundle_at_root_level:
        out_path = zenodo_bundle_path / file.name
        shutil.copy2(file, out_path)
        bundled_files.append(out_path.relative_to(zenodo_bundle_path))

    def name_contains_filter(fp: Path, blacklist: list[str]) -> bool:
        return any(s in fp.name for s in blacklist)

    universal_filter = partial(name_contains_filter, blacklist=["DS_Store"])
    data_out_filter = partial(name_contains_filter, blacklist=[".complete", "chk"])

    directories_to_bundle = [
        DirectoryBundlingSpecs(
            dir=Path("magicc"),
            level=1,
            filters=[
                universal_filter,
                partial(
                    name_contains_filter,
                    blacklist=[
                        "cmip7-ghgs",
                        "openscm-runner",
                    ],
                ),
            ],
        ),
        DirectoryBundlingSpecs(
            dir=Path("notebooks"),
            level=0,
            filters=[
                universal_filter,
                partial(name_contains_filter, blacklist=[".ipynb"]),
            ],
        ),
        DirectoryBundlingSpecs(
            dir=Path("scripts"),
            level=0,
            filters=[
                universal_filter,
            ],
        ),
        DirectoryBundlingSpecs(
            dir=Path("src"),
            level=0,
            filters=[
                universal_filter,
                lambda p: "egg-info" in p.name,
            ],
        ),
        DirectoryBundlingSpecs(
            dir=Path("data/raw"),
            level=1,
            filters=[
                universal_filter,
                partial(
                    name_contains_filter,
                    blacklist=[
                        "historical-ghg-concs",  # Backed by DOI
                        "historical-ghg-data-interim",  # Backed by DOI
                        "western-et-al-2024",  # Backed by DOI
                    ],
                ),
            ],
        ),
        DirectoryBundlingSpecs(
            dir=Path("data/interim"),
            level=1,
            filters=[universal_filter, data_out_filter],
        ),
        DirectoryBundlingSpecs(
            dir=Path("data/processed"),
            level=1,
            filters=[universal_filter, data_out_filter],
        ),
    ]

    for dbs in directories_to_bundle:
        written_tars = create_level_aware_tar(
            original_bundle_path=original_bundle_path,
            current_level_path_rel_to_original_bundle_path=dbs.dir,
            zenodo_bundle_path=zenodo_bundle_path,
            current_level=0,
            bundle_at_level=dbs.level,
            filters=dbs.filters,
        )
        bundled_files.extend(written_tars)

    return bundled_files


def get_draft_deposition_id_and_version(esgf_ready_files_root: Path) -> tuple[str, str]:
    dois = []
    versions = []
    for nc_file in tqdm.auto.tqdm(esgf_ready_files_root.rglob("**/*.nc"), desc="Retrieving metadata from netCDF files"):
        with netCDF4.Dataset(nc_file) as ds:
            dois.append(ds.getncattr("doi"))
            versions.append(ds.getncattr("source_version"))

    if len(set(dois)) != 1:
        msg = f"More than one DOI in the files, {set(dois)=}"
        raise ValueError(msg)

    draft_deposition_id = dois[0].replace("10.5281/zenodo.", "")

    if len(set(versions)) != 1:
        msg = f"More than one version in the files, {set(versions)=}"
        raise ValueError(msg)

    version = versions[0]

    return draft_deposition_id, version


def write_zenodo_readme(out_path: Path, version: str, zenodo_record_id: str, zenodo_bundle_files: list[Path]) -> Path:
    """
    Write README for the zenodo page

    Parameters
    ----------
    out_path
        Output path to write the README

    version
        Version to put in the README

    zenodo_record_id
        The record ID of the zenodo upload in which this README will appear

    zenodo_bundle_files
        Files that are included in the zenodo bundle

    Returns
    -------
    :
        Written path
    """
    zenodo_bundle_files_formatted = "\n        ".join(f'"{f.name}",' for f in zenodo_bundle_files)
    readme_content = f'''# CMIP7 ScenarioMIP GHG Concentrations

This archive contains the workflow and outputs
used to create the CMIP7 ScenarioMIP GHG concentrations version {version}.

To reproduce the results, please follow the steps below.

## Download data

Download all the data files you need using the script below

```python
"""
Get files from zenodo and extract them
"""

import importlib.util
import tarfile
from pathlib import Path

# If you don't have pooch already, get it with `pip install pooch`
# (we recommend using a a virtual environment
# rather than installing globally, but ultimately it's up to you).

# If you want progress bars, `pip install tqdm`
has_tqdm = importlib.util.find_spec("tqdm")


def main():
    record_id = "{zenodo_record_id}"
    out_path = Path(record_id)

    for file in [
        {zenodo_bundle_files_formatted}
    ]:
        downloaded = pooch.retrieve(
            f"https://zenodo.org/records/{{record_id}}/files/{{file}}?download=1",
            fname=file,
            path=out_path,
            progressbar=has_tqdm is not None,
        )

        downloaded = out_path / file
        if file.endswith(".tar.gz"):
            with tarfile.open(downloaded, "r:gz") as tar:
                tar.extractall(path=out_path)

    print(f"The downloaded data is available in {{out_path}}")


if __name__ == "__main__":
    main()
```

## Run the workflow

Move into the directory in which the data files were downloaded,
then run `pixi run prefect server start`.
Open a new terminal, move into the directory in which the data files were downloaded again,
then run `bash scripts/create-latest-set-of-concentration-files.sh`.
This will run the workflow and create the ESGF-equivalent* files.

*We say ESGF-equivalent because each file has the time at which it was written in its metadata.
Hence, the files will not be identical, although the data within them should be.
You can confirm this with `notebooks/2001_compare-local-to-esgf.py`.
'''

    with open(out_path, "w") as fh:
        fh.write(readme_content)

    return out_path


def main(
    bundle_path: Annotated[Path, typer.Argument(help="Path to the bundle to prepare for zenodo")],
    zenodo_bundle_root_path: Annotated[Path, typer.Option(help="Root path in which to save the Zenodo bundle")] = Path(
        "zenodo-bundles"
    ),
    logging_level: Annotated[str, typer.Option(help="Logging level to use")] = "INFO",
    zenodo_metadata_file: Annotated[
        str, typer.Option(help="Name of the file in which the zenodo metadata was written")
    ] = "zenodo.json",
    reserved_zenodo_doi_file: Annotated[
        str, typer.Option(help="Name of the file in which to write the reserved Zenodo DOI")
    ] = "reserved-zenodo-doi.txt",
    # dependencies_table_file: Annotated[
    #     Path, typer.Option(help="Path from which to read the dependencies table")
    # ] = Path("data/processed/dependencies.db"),
) -> None:
    load_dotenv()

    logger.configure(handlers=[dict(sink=sys.stderr, level=logging_level)])
    logger.enable("openscm_zenodo")

    bundle_id = bundle_path.parts[-1]
    zenodo_bundle_path = zenodo_bundle_root_path / bundle_id

    zenodo_bundle_path.mkdir(exist_ok=True, parents=True)

    zenodo_bundle_files = create_zenodo_bundle(zenodo_bundle_path=zenodo_bundle_path, original_bundle_path=bundle_path)

    draft_deposition_id, version = get_draft_deposition_id_and_version(
        bundle_path / "data/processed/esgf-ready/input4MIPs"
    )
    with open(zenodo_bundle_path / reserved_zenodo_doi_file, "w") as fh:
        fh.write(draft_deposition_id)

    write_zenodo_readme(
        out_path=zenodo_bundle_path / "README.md",
        version=version,
        zenodo_record_id=draft_deposition_id,
        zenodo_bundle_files=zenodo_bundle_files,
    )


if __name__ == "__main__":
    typer.run(main)
