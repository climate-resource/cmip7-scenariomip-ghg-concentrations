"""
Generate the concentration files
"""

import multiprocessing
from pathlib import Path
from typing import Annotated

import typer

from cmip7_scenariomip_ghg_generation.main_flow import create_scenariomip_ghgs

REPO_ROOT_DIR = Path(__file__).parents[1]
OUTPUT_BUNDLES_ROOT_DIR = REPO_ROOT_DIR / "output-bundles"
REPO_DATA_DIR = REPO_ROOT_DIR / "data" / "raw"

ALL_GHGS = [
    "c2f6",
    "c3f8",
    "c4f10",
    "c5f12",
    "c6f14",
    "c7f16",
    "c8f18",
    "cc4f8",
    "ccl4",
    "cf4",
    "cfc11",
    "cfc113",
    "cfc114",
    "cfc115",
    "cfc11eq",
    "cfc12",
    "cfc12eq",
    "ch2cl2",
    "ch3br",
    "ch3ccl3",
    "ch3cl",
    "ch4",
    "chcl3",
    "co2",
    "halon1211",
    "halon1301",
    "halon2402",
    "hcfc141b",
    "hcfc142b",
    "hcfc22",
    "hfc125",
    "hfc134a",
    "hfc134aeq",
    "hfc143a",
    "hfc152a",
    "hfc227ea",
    "hfc23",
    "hfc236fa",
    "hfc245fa",
    "hfc32",
    "hfc365mfc",
    "hfc4310mee",
    "n2o",
    "nf3",
    "sf6",
    "so2f2",
]


def main(
    ghg: Annotated[list[str], typer.Option(help="GHG to process")] = ALL_GHGS,
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
    n_workers: Annotated[
        int, typer.Option(help="Number of workers to use for parallel work")
    ] = multiprocessing.cpu_count(),
) -> tuple[Path, ...]:
    """
    Generate the CMIP7 ScenarioMIP greenhouse gas concentration files
    """
    ghgs = tuple(ghg)
    # Lots of things here that can't be passed from the CLI.
    # Honestly, making it all run from the CLI is an unnecessary headache.
    # If you want to change it, just edit this script.
    raw_notebooks_root_dir = REPO_ROOT_DIR / "notebooks"

    output_bundle_root_dir = OUTPUT_BUNDLES_ROOT_DIR / run_id
    data_root = output_bundle_root_dir / "data"
    data_raw_root = data_root / "raw"
    data_interim_root = data_root / "interim"

    executed_notebooks_dir = output_bundle_root_dir / "notebooks-executed"

    ### Historical GHG
    cmip7_historical_ghg_concentration_source_id = "CR-CMIP-1-0-0"
    cmip7_historical_ghg_concentration_data_root_dir = data_raw_root / "historical-ghg-concs"
    cmip7_historical_seasonality_lat_gradient_info_raw_file_url = (
        "https://zenodo.org/records/14892947/files/data--interim.tar.gz?download=1"
    )
    cmip7_historical_seasonality_lat_gradient_info_raw_file = data_raw_root / "historical-ghg-data-interim-info.tar.gz"
    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir = data_raw_root / "historical-ghg-data-interim"

    ### WMO 2022 stuff
    wmo_raw_data_path = REPO_DATA_DIR / "wmo-2022" / "MixingRatiosCMIP7_20250210.xlsx"
    # Save as feather as this is an interim product
    wmo_cleaned_data_path = data_interim_root / "wmo-2022" / "cleaned-mixing-ratios.feather"

    ### Western et al. 2024 stuff
    western_et_al_2024_download_url = "https://zenodo.org/records/10782689/files/Projections.zip?download=1"
    western_et_al_2024_raw_tar_file = data_raw_root / "western-et-al-2024" / "Projections.zip"
    western_et_al_2024_extract_path = data_raw_root / "western-et-al-2024" / "projections"
    western_et_al_2024_extracted_file_of_interest = Path("Projections") / "hcfc_projections_v2.csv"
    western_et_al_2024_cleaned_data_path = data_interim_root / "western-et-al-2024" / "cleaned-mixing-ratios.feather"
    western_et_al_2024_extended_data_path = data_interim_root / "western-et-al-2024" / "extended-mixing-ratios.feather"

    ### Interim outputs
    annual_mean_dir = data_interim_root / "annual-means"
    monthly_mean_dir = data_interim_root / "monthly-means"

    create_scenariomip_ghgs(
        ghgs=ghgs,
        run_id=run_id,
        n_workers=n_workers,
        raw_notebooks_root_dir=raw_notebooks_root_dir,
        executed_notebooks_dir=executed_notebooks_dir,
        cmip7_historical_ghg_concentration_source_id=cmip7_historical_ghg_concentration_source_id,
        cmip7_historical_ghg_concentration_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
        cmip7_historical_seasonality_lat_gradient_info_raw_file_url=cmip7_historical_seasonality_lat_gradient_info_raw_file_url,
        cmip7_historical_seasonality_lat_gradient_info_raw_file=cmip7_historical_seasonality_lat_gradient_info_raw_file,
        cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir=cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir,
        wmo_raw_data_path=wmo_raw_data_path,
        wmo_cleaned_data_path=wmo_cleaned_data_path,
        western_et_al_2024_download_url=western_et_al_2024_download_url,
        western_et_al_2024_raw_tar_file=western_et_al_2024_raw_tar_file,
        western_et_al_2024_extract_path=western_et_al_2024_extract_path,
        western_et_al_2024_extracted_file_of_interest=western_et_al_2024_extracted_file_of_interest,
        western_et_al_2024_cleaned_data_path=western_et_al_2024_cleaned_data_path,
        western_et_al_2024_extended_data_path=western_et_al_2024_extended_data_path,
        annual_mean_dir=annual_mean_dir,
        monthly_mean_dir=monthly_mean_dir,
    )


if __name__ == "__main__":
    typer.run(main)
