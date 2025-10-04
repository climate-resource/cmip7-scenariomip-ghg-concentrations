"""
Generate the concentration files
"""

from pathlib import Path
from typing import Annotated

import click
import typer
from attrs import evolve
from input4mips_validation.cvs.loading import load_cvs_known_loader
from input4mips_validation.cvs.loading_raw import get_raw_cvs_loader
from pandas_openscm.io import load_timeseries_csv

from cmip7_scenariomip_ghg_generation.input4mips_cvs_helpers import create_source_id
from cmip7_scenariomip_ghg_generation.main_flow import create_scenariomip_ghgs
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo

REPO_ROOT_DIR = Path(__file__).parents[1]
OUTPUT_BUNDLES_ROOT_DIR = REPO_ROOT_DIR / "output-bundles"
REPO_RAW_DATA_DIR = REPO_ROOT_DIR / "data" / "raw"

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
    "halon1202",
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


def main(  # noqa: PLR0912, PLR0913, PLR0915
    emissions_file: Annotated[
        Path, typer.Option(help="Emissions file received from the emissions harmonisation team")
    ] = (REPO_RAW_DATA_DIR / "input-scenarios" / "0009-zn_0003_0003_0002_harmonised-emissions-up-to-sillicone.csv"),
    scenarios_to_run: Annotated[
        str,
        typer.Option(
            click_type=click.Choice(["all", "markers", "custom"]),
            help="""Scenarios to run

Options:

- all: run all scenarios
- markers: run only the markers
- custom: run whatever custom selection is in the script""",
        ),
    ] = "markers",
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
    magicc_version_to_run: Annotated[list[str], typer.Option(help="MAGICC version to run")] = [
        "MAGICCv7.6.0a3",
        "MAGICCv7.5.3",
    ],
    magicc_root_folder: Annotated[Path, typer.Option(help="Root folder for MAGICC versions")] = REPO_ROOT_DIR
    / "magicc",
    esgf_version: Annotated[
        str,
        typer.Option(help="""Version to use when writing the files for ESGF"""),
    ] = "0.0.1",
    input4mips_cvs_source: Annotated[
        str,
        typer.Option(help="""Source for the input4MIPs CVs"""),
    ] = "gh:c75a54d0af36dbedf654ad2eeba66e9c1fbce2a2",
    n_workers: Annotated[
        int,
        typer.Option(
            help="""Number of task runner workers to use

Note that these are threaded runners.
We have tried to set things up
(with the various other `n-workers-*` options)
to ensure that jobs that need to be processed in parallel
are actually handled correctly.
However, as a result, they also have different worker numbers."""
        ),
    ] = 1,
    n_workers_multiprocessing: Annotated[
        int,
        typer.Option(
            help="""Number of multiprocessing workers to use

These are only used for jobs which have been set up to support multiprocessing.
Not that this does not apply to the MAGICC running multiprocessing,
because that is more complicated."""
        ),
    ] = 1,
    n_workers_multiprocessing_magicc: Annotated[
        int,
        typer.Option(
            help="""Number of multiprocessing workers to use for the MAGICC running notebooks

Each notebook then gets `--n-workers-per-magicc-notebook`,
so the total number of workers used for running MAGICC stuff
will be the *product* of the two.
Be careful and don't crash your computer."""
        ),
    ] = 1,
    n_workers_per_magicc_notebook: Annotated[
        int,
        typer.Option(
            help="""Number of MAGICC workers to use in each MAGICC-running notebook

Up to `--n-workers-multiprocessing-magicc`
MAGICC processing notebooks are run in parallel,
so the total number of workers used for running MAGICC stuff
will be the *product* of the two levels of parallelisation.
Be careful and don't crash your computer."""
        ),
    ] = 1,
) -> tuple[Path, ...]:
    """
    Generate the CMIP7 ScenarioMIP greenhouse gas concentration files
    """
    # # TODO: activate this
    # load_dotenv()

    ghgs = tuple(ghg)
    magicc_versions_to_run = tuple(magicc_version_to_run)

    # Lots of things here that can't be passed from the CLI.
    # Honestly, making it all run from the CLI is an unnecessary headache.
    # If you want to change it, just edit this script.
    fossil_bio_split_file = emissions_file.parent / emissions_file.name.replace(
        "up-to-sillicone", "fossil-biosphere-aggregation"
    )
    if not fossil_bio_split_file.exists():
        raise FileNotFoundError(fossil_bio_split_file)

    markers = (
        # (model, scenario, cmip7 experiment name)
        # Decision: https://github.com/WCRP-CMIP/CMIP7-CVs/discussions/1#discussioncomment-14585785
        # vl likely to be finalised first
        ("REMIND-MAgPIE 3.5-4.11", "SSP1 - Very Low Emissions", "vl"),
        # ("AIM 3.0", "SSP2 - Low Overshoot_e", "ln"),
        # ("MESSAGEix-GLOBIOM-GAINS 2.1-M-R12", "SSP2 - Low Emissions", "l"),
        # ("COFFEE 1.6", "SSP2 - Medium-Low Emissions", "ml"),
        # ("IMAGE 3.4", "SSP2 - Medium Emissions", "m"),
        # ("WITCH 6.0", "SSP5 - Medium-Low Emissions_a", "hl"),
        # ("GCAM 7.1 scenarioMIP", "SSP3 - High Emissions", "h"),
    )

    # Choices here are quite arbitrary
    # and based on expert judgement.
    magicc_based_ghgs_projection_method = {
        "co2": "gradient-aware-harmonisation",
        "ch4": "gradient-aware-harmonisation",
        "n2o": "gradient-aware-harmonisation",
        "c2f6": "one-box",
        "c3f8": "one-box",
        "c4f10": "one-box",
        "c5f12": "one-box",
        "c6f14": "one-box",
        "c7f16": "one-box",
        "c8f18": "one-box",
        "cc4f8": "one-box",
        "cf4": "one-box",
        "ch2cl2": "one-box",
        "chcl3": "one-box",
        "hfc125": "one-box",
        "hfc134a": "gradient-aware-harmonisation",
        "hfc143a": "one-box",
        "hfc152a": "gradient-aware-harmonisation",
        "hfc227ea": "one-box",
        "hfc23": "gradient-aware-harmonisation",
        "hfc236fa": "one-box",
        "hfc245fa": "gradient-aware-harmonisation",
        "hfc32": "gradient-aware-harmonisation",
        "hfc365mfc": "gradient-aware-harmonisation",
        "hfc4310mee": "one-box",
        "nf3": "one-box",
        "sf6": "one-box",
        "so2f2": "one-box",
    }

    emissions_batch_id = emissions_file.name.split("_harmonised")[0]

    esgf_institution_id = "CR"

    # Check that all the source IDs we will create
    # are indeed already registered
    raw_cvs_loader = get_raw_cvs_loader(
        input4mips_cvs_source,
        # # Can force updates if you need using this
        # force_download=True,
    )
    cvs = load_cvs_known_loader(raw_cvs_loader)
    for marker_info in markers:
        marker_source_id = create_source_id(
            esgf_institution_id=esgf_institution_id,
            cmip_scenario_name=marker_info[-1],
            esgf_version=esgf_version,
        )
        # Check that source ID is in the CVs
        if marker_source_id not in cvs.source_id_entries.source_ids:
            msg = (
                f"{marker_source_id} is not registered in "
                f"input4MIPs CVs {input4mips_cvs_source}. "
                "Please push an update to input4MIPs CVs, "
                "then use that update as your `input4mips_cvs_source"
            )
            raise AssertionError(msg)

    raw_notebooks_root_dir = REPO_ROOT_DIR / "notebooks"

    output_bundle_root_dir = OUTPUT_BUNDLES_ROOT_DIR / run_id
    data_root = output_bundle_root_dir / "data"
    data_raw_root = data_root / "raw"
    data_interim_root = data_root / "interim"
    data_processed_root = data_root / "processed"

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
    wmo_raw_data_path = REPO_RAW_DATA_DIR / "wmo-2022" / "MixingRatiosCMIP7_20250210.xlsx"
    # Save as feather as this is an interim product
    wmo_cleaned_data_path = data_interim_root / "wmo-2022" / "cleaned-mixing-ratios.feather"

    ### Western et al. 2024 stuff
    western_et_al_2024_download_url = "https://zenodo.org/records/10782689/files/Projections.zip?download=1"
    western_et_al_2024_raw_tar_file = data_raw_root / "western-et-al-2024" / "Projections.zip"
    western_et_al_2024_extract_path = data_raw_root / "western-et-al-2024" / "projections"
    western_et_al_2024_extracted_file_of_interest = Path("Projections") / "hcfc_projections_v2.csv"
    western_et_al_2024_cleaned_data_path = data_interim_root / "western-et-al-2024" / "cleaned-mixing-ratios.feather"

    ### Interim outputs
    annual_mean_dir = data_interim_root / "annual-means"
    monthly_mean_dir = data_interim_root / "monthly-means"
    seasonality_dir = data_interim_root / "seasonality"
    lat_gradient_dir = data_interim_root / "latitudinal-gradient"
    emissions_split_dir = data_interim_root / "input-emissions" / emissions_batch_id
    inverse_emission_dir = data_interim_root / "inverse-emissions"
    emissions_complete_dir = data_interim_root / "complete-emissions"
    magicc_output_db_dir = data_interim_root / "magicc-output" / "db"
    magicc_db_backend_str = "feather"
    fossil_bio_split_interim_dir = data_interim_root / "fossil-biosphere-split"
    single_variable_dir = data_interim_root / "single-variable-files"
    plot_complete_dir = data_interim_root / "plot-complete"

    ### Final outputs
    esgf_ready_root_dir = data_processed_root / "esgf-ready"

    ### Scenario processing and set up
    all_emissions = load_timeseries_csv(
        emissions_file,
        index_columns=["model", "scenario", "region", "variable", "unit"],
        out_columns_type=int,
        out_columns_name="year",
    )

    history_loc = all_emissions.index.get_level_values("scenario") == "historical"
    scenarios = all_emissions.loc[~history_loc]
    all_model_scenarios = scenarios.index.droplevel(
        all_emissions.index.names.difference(["model", "scenario"])
    ).drop_duplicates()

    scenario_infos_l = [
        ScenarioInfo(
            cmip_scenario_name=None,
            model=model,
            scenario=scenario,
        )
        for model, scenario in all_model_scenarios.reorder_levels(["model", "scenario"])
    ]
    for model, scenario, cmip_scenario_name in markers:
        for i, si in enumerate(scenario_infos_l):
            if si.model == model and si.scenario == scenario:
                break

        else:
            msg = f"{model=} {scenario=} not found in input model-scenario options"
            raise AssertionError(msg)

        scenario_infos_l[i] = evolve(si, cmip_scenario_name=cmip_scenario_name)

    # Double check
    for model, scenario, cmip_scenario_name in markers:
        for i, si in enumerate(scenario_infos_l):
            if si.model == model and si.scenario == scenario:
                if si.cmip_scenario_name != cmip_scenario_name:
                    msg = f"{model=} {scenario=} should have {cmip_scenario_name=} but it has {si.cmip_scenario_name=}"
                    raise AssertionError(msg)

                break

        else:
            msg = f"{model=} {scenario=} marker not set correctly"
            raise AssertionError(msg)

    if scenarios_to_run == "all":
        scenario_infos = tuple(scenario_infos_l)

    elif scenarios_to_run == "markers":
        scenario_infos = tuple(v for v in scenario_infos_l if v.cmip_scenario_name is not None)

    elif scenarios_to_run == "custom":
        scenario_infos = tuple(
            v
            for v in scenario_infos_l
            if (v.cmip_scenario_name in ["vllo", "l"])
            or (v.model == "WITCH 6.0" and v.scenario == "SSP1 - Very Low Emissions")
        )

    create_scenariomip_ghgs(
        ghgs=ghgs,
        emissions_file=emissions_file,
        scenario_infos=scenario_infos,
        run_id=run_id,
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
        annual_mean_dir=annual_mean_dir,
        monthly_mean_dir=monthly_mean_dir,
        seasonality_dir=seasonality_dir,
        inverse_emission_dir=inverse_emission_dir,
        lat_gradient_dir=lat_gradient_dir,
        emissions_split_dir=emissions_split_dir,
        emissions_complete_dir=emissions_complete_dir,
        magicc_versions_to_run=magicc_versions_to_run,
        magicc_root_folder=magicc_root_folder,
        magicc_output_db_dir=magicc_output_db_dir,
        magicc_db_backend_str=magicc_db_backend_str,
        magicc_based_ghgs_projection_method=magicc_based_ghgs_projection_method,
        fossil_bio_split_file=fossil_bio_split_file,
        fossil_bio_split_interim_dir=fossil_bio_split_interim_dir,
        single_variable_dir=single_variable_dir,
        plot_complete_dir=plot_complete_dir,
        esgf_ready_root_dir=esgf_ready_root_dir,
        esgf_version=esgf_version,
        esgf_institution_id=esgf_institution_id,
        input4mips_cvs_source=input4mips_cvs_source,
        n_workers=n_workers,
        n_workers_multiprocessing=n_workers_multiprocessing,
        n_workers_multiprocessing_magicc=n_workers_multiprocessing_magicc,
        n_workers_per_magicc_notebook=n_workers_per_magicc_notebook,
    )


if __name__ == "__main__":
    typer.run(main)
