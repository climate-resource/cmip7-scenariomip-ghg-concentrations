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
) -> None:
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


def create_level_aware_tar(  # noqa: PLR0913
    original_bundle_path: Path,
    current_level_path_rel_to_original_bundle_path: Path,
    zenodo_bundle_path: Path,
    bundle_at_level: int,
    filters: list[Callable[[Path], bool]],
    current_level: int,
) -> None:
    if bundle_at_level == current_level:
        create_tar_file(
            original_bundle_path=original_bundle_path,
            current_level_path_rel_to_original_bundle_path=current_level_path_rel_to_original_bundle_path,
            zenodo_bundle_path=zenodo_bundle_path,
            filters=filters,
        )

        return

    level_files = []
    for level_component in (original_bundle_path / current_level_path_rel_to_original_bundle_path).iterdir():
        if any(filter(level_component) for filter in filters):
            print(f"    - Not bundling {level_component}")
            continue

        if level_component.is_dir():
            create_level_aware_tar(
                original_bundle_path=original_bundle_path,
                current_level_path_rel_to_original_bundle_path=level_component.relative_to(original_bundle_path),
                zenodo_bundle_path=zenodo_bundle_path,
                current_level=current_level + 1,
                bundle_at_level=bundle_at_level,
                filters=filters,
            )

        else:
            level_files.append(level_component)

    if level_files:
        create_tar_file(
            original_bundle_path=original_bundle_path,
            current_level_path_rel_to_original_bundle_path=current_level_path_rel_to_original_bundle_path,
            zenodo_bundle_path=zenodo_bundle_path,
            filters=filters,
            files_only=True,
        )


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


def create_zenodo_bundle(zenodo_bundle_path: Path, original_bundle_path: Path) -> None:
    files_to_bundle_at_root_level = (
        original_bundle_path / "Makefile",
        original_bundle_path / "README.md",
        original_bundle_path / "pixi.lock",
        original_bundle_path / "pyproject.toml",
        original_bundle_path / "zenodo.json",
        # *original_bundle_path.glob("*.yaml"),
    )

    for file in files_to_bundle_at_root_level:
        shutil.copyfile(file, zenodo_bundle_path / file.name)

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
                lambda p: not p.name.endswith(".py"),
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
        create_level_aware_tar(
            original_bundle_path=original_bundle_path,
            current_level_path_rel_to_original_bundle_path=dbs.dir,
            zenodo_bundle_path=zenodo_bundle_path,
            current_level=0,
            bundle_at_level=dbs.level,
            filters=dbs.filters,
        )


def get_draft_deposition_id(esgf_ready_files_root: Path) -> str:
    dois = []
    for nc_file in tqdm.auto.tqdm(esgf_ready_files_root.rglob("**/*.nc"), desc="Retrieving DOIs from netCDF files"):
        with netCDF4.Dataset(nc_file) as ds:
            dois.append(ds.getncattr("doi"))

    if len(set(dois)) != 1:
        msg = f"More than one DOI in the files, {set(dois)=}"
        raise ValueError(msg)

    draft_deposition_id = dois[0].replace("10.5281/zenodo.", "")

    return draft_deposition_id


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
    # zenodo_interactor = ZenodoInteractor(token=os.environ["ZENODO_TOKEN"])

    # with open(bundle_path / zenodo_metadata_file) as fh:
    #     zenodo_metadata = json.load(fh)

    # with open(bundle_path / f"{bundle_id}-config.yaml") as fh:
    #     config = yaml.safe_load(fh)

    # draft_deposition_id = config["doi"].split("10.5281/zenodo.")[1]

    # # Helpful if you need to work out how identifiers look in Zenodo JSON
    # tmp = zenodo_interactor.get_metadata("14892947")
    # tmp["metadata"]["related_identifiers"]

    # db_connection = sqlite3.connect(bundle_path / dependencies_table_file)
    # sources = pd.read_sql("SELECT * FROM source", con=db_connection)
    # dependencies = pd.read_sql("SELECT * FROM dependencies", con=db_connection)
    # db_connection.close()

    # sources_used = sources[sources["short_name"].isin(dependencies["short_name"])]

    # zenodo_metadata_incl_refs = add_dependencies_to_metadata(
    #     dependencies_table=sources_used,
    #     metadata=zenodo_metadata,
    # )

    zenodo_bundle_path.mkdir(exist_ok=True, parents=True)

    create_zenodo_bundle(zenodo_bundle_path=zenodo_bundle_path, original_bundle_path=bundle_path)

    draft_deposition_id = get_draft_deposition_id(bundle_path / "data/processed/esgf-ready/input4MIPs")
    with open(zenodo_bundle_path / reserved_zenodo_doi_file, "w") as fh:
        fh.write(draft_deposition_id)


if __name__ == "__main__":
    typer.run(main)
