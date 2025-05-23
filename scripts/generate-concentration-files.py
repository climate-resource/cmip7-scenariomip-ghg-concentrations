"""
Generate the concentration files
"""

from pathlib import Path
from typing import Annotated

import typer
from cmip7_scenariomip_ghg_generation.main_flow import create_scenariomip_ghgs_wrapper

REPO_ROOT_DIR = Path(__file__).parents[1]
DATA_ROOT = REPO_ROOT_DIR / "data"
DATA_RAW_ROOT = DATA_ROOT / "raw"
DATA_INTERIM_ROOT = DATA_ROOT / "interim"


def main(
    run_id: Annotated[
        str,
        typer.Option(
            help="""ID for this run

If you use an already existing run ID,
this will enable helpful things like caching.
If you use a new run ID,
this will lead to a new run being done
(i.e. there will be no caching)."""
        ),
    ] = "dev-test",
) -> tuple[Path, ...]:
    """
    Generate the CMIP7 ScenarioMIP greenhouse gas concentration files
    """
    # Lots of things here that can't be passed from the CLI.
    # Honestly, making it all run from the CLI is an unnecessary headache.
    # If you want to change it, just edit this script.

    ### WMO 2022 stuff
    wmo_raw_data_path = Path(
        DATA_RAW_ROOT / "wmo-2022" / "MixingRatiosCMIP7_20250210.xlsx",
    )
    # Save as feather as this is an interim product
    wmo_extracted_data_path = Path(
        DATA_INTERIM_ROOT / "wmo-2022" / "extracted-mixing-ratios.feather",
    )

    create_scenariomip_ghgs_wrapper(
        run_id=run_id,
        wmo_raw_data_path=wmo_raw_data_path,
        wmo_extracted_data_path=wmo_extracted_data_path,
    )


if __name__ == "__main__":
    typer.run(main)
