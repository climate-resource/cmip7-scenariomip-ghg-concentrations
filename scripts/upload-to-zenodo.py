"""
Upload a bundle to Zenodo
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Annotated, Any

import typer
from dotenv import load_dotenv
from loguru import logger
from openscm_zenodo.zenodo import ZenodoInteractor

# ruff: noqa: D101, D102, D103


def upload_to_zenodo(  # noqa: PLR0913
    zenodo_bundle_path: Path,
    publish: bool,
    zenodo_interactor: ZenodoInteractor,
    draft_deposition_id: str,
    metadata: dict[str, Any],
    reserved_zenodo_doi_file: str,
) -> None:
    zenodo_interactor.update_metadata(deposition_id=draft_deposition_id, metadata=metadata)

    zenodo_interactor.remove_all_files(deposition_id=draft_deposition_id)

    bucket_url = zenodo_interactor.get_bucket_url(deposition_id=draft_deposition_id)
    for file in zenodo_bundle_path.iterdir():
        if file.name == reserved_zenodo_doi_file:
            print(f"Not uploading: {reserved_zenodo_doi_file}")
            continue

        zenodo_interactor.upload_file_to_bucket_url(
            file,
            bucket_url=bucket_url,
        )

    if publish:
        zenodo_interactor.publish(deposition_id=draft_deposition_id)
        print(f"Published the new record at https://zenodo.org/records/{draft_deposition_id}")

    else:
        print(f"You can preview the draft upload at https://zenodo.org/uploads/{draft_deposition_id}")


def main(
    bundle_path: Annotated[Path, typer.Argument(help="Path to the bundle to upload")],
    publish: Annotated[bool, typer.Option(help="Should we publish the uploaded data?")] = False,
    logging_level: Annotated[str, typer.Option(help="Logging level to use")] = "INFO",
    zenodo_metadata_file: Annotated[
        str, typer.Option(help="Name of the file in which the zenodo metadata was written")
    ] = "zenodo.json",
    reserved_zenodo_doi_file: Annotated[
        str, typer.Option(help="Name of the file in which the reserved Zenodo DOI was saved")
    ] = "reserved-zenodo-doi.txt",
) -> None:
    load_dotenv()

    logger.configure(handlers=[dict(sink=sys.stderr, level=logging_level)])
    logger.enable("openscm_zenodo")

    zenodo_interactor = ZenodoInteractor(token=os.environ["ZENODO_TOKEN"])

    with open(bundle_path / zenodo_metadata_file) as fh:
        zenodo_metadata = json.load(fh)

    with open(bundle_path / reserved_zenodo_doi_file) as fh:
        draft_deposition_id = fh.read().strip()

    # # Helpful if you need to work out how identifiers look in Zenodo JSON
    # published_deposition_id = "14892947"
    # tmp = zenodo_interactor.get_metadata(published_deposition_id)
    # print(tmp["metadata"]["related_identifiers"])

    upload_to_zenodo(
        bundle_path,
        publish=publish,
        zenodo_interactor=zenodo_interactor,
        draft_deposition_id=draft_deposition_id,
        metadata=zenodo_metadata,
        reserved_zenodo_doi_file=reserved_zenodo_doi_file,
    )

    print(
        "\n".join(
            [
                "Next steps:",
                "",
                "- update affiliations (can't have multiple from zenodo.json)",
                "- update grants (can't upload from zenodo.json)",
            ]
        )
    )


if __name__ == "__main__":
    typer.run(main)
