"""
Main flow
"""

import multiprocessing.pool
from contextlib import nullcontext
from functools import partial
from pathlib import Path

from prefect import flow
from prefect.futures import PrefectFuture, wait
from prefect.states import Completed
from prefect.task_runners import ThreadPoolTaskRunner

from cmip7_scenariomip_ghg_generation.prefect_helpers import submit_output_aware
from cmip7_scenariomip_ghg_generation.prefect_tasks import (
    clean_wmo_data,
    compile_inverse_emissions,
    download_cmip7_historical_ghg_concentrations,
    download_file,
    extend_western_et_al_2024,
    extract_tar,
    get_doi,
    get_magicc_version_info,
    get_western_et_al_2024_clean,
    make_complete_scenario,
    run_magicc,
    split_input_emissions_into_individual_files,
)
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo
from cmip7_scenariomip_ghg_generation.single_concentration_projection_flow import (
    create_scenariomip_ghgs_single_concentration_projection,
)


def create_scenariomip_ghgs_flow(  # noqa: PLR0912, PLR0913, PLR0915
    ghgs: tuple[str, ...],
    emissions_file: Path,
    scenario_infos: tuple[ScenarioInfo, ...],
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    cmip7_historical_seasonality_lat_gradient_info_raw_file_url: str,
    cmip7_historical_seasonality_lat_gradient_info_raw_file: Path,
    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir: Path,
    wmo_raw_data_path: Path,
    wmo_cleaned_data_path: Path,
    western_et_al_2024_download_url: str,
    western_et_al_2024_raw_tar_file: Path,
    western_et_al_2024_extract_path: Path,
    western_et_al_2024_extracted_file_of_interest: Path,
    western_et_al_2024_cleaned_data_path: Path,
    annual_mean_dir: Path,
    monthly_mean_dir: Path,
    seasonality_dir: Path,
    inverse_emission_dir: Path,
    lat_gradient_dir: Path,
    emissions_split_dir: Path,
    emissions_complete_dir: Path,
    magicc_versions_to_run: tuple[str, ...],
    magicc_root_folder: Path,
    n_magicc_workers: int,
    magicc_output_dir: Path,
    esgf_ready_writing_pool: multiprocessing.pool.Pool | None,
    esgf_ready_root_dir: Path,
    esgf_version: str,
    esgf_institution_id: str,
    input4mips_cvs_source: str,
) -> tuple[Path, ...] | tuple[Path | PrefectFuture, ...]:
    """
    Create the ScenarioMIP GHG concentrations

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

    emissions_file
        File containing emissions received from the harmonisation team

    scenario_infos
        Scenario information

    raw_notebooks_root_dir
        Root directory for raw notebooks

    executed_notebooks_dir
        Path in which to write executed notebooks

    cmip7_historical_ghg_concentration_source_id
        Source ID (unique identifier) for historical CMIP7 GHG concentrations

    cmip7_historical_ghg_concentration_data_root_dir
        Root directory for saving CMIP7 historical GHG concentrations

    cmip7_historical_seasonality_lat_gradient_info_raw_file_url
        URL from which to download the CMIP7 historical GHG concentrations seasonality and lat. gradient info

    cmip7_historical_seasonality_lat_gradient_info_raw_file
        File in which to save the CMIP7 historical GHG concentrations seasonality and lat. gradient info

    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir
        Root directory in which to extract `cmip7_historical_seasonality_lat_gradient_info_raw_file`

    wmo_raw_data_path
        Path to raw WMO data

    wmo_cleaned_data_url
        Path in which to extract the WMO data

    western_et_al_2024_download_url
        URL from which to download the raw Western et al. (2024) data

    western_et_al_2024_raw_tar_file
        Path in which to download the raw Western et al. (2024) data

    western_et_al_2024_extract_path
        Path in which to extract the raw Western et al. (2024) data

    western_et_al_2024_extracted_file_of_interest
        File of interest from the extracted Western et al. (2024) data

    western_et_al_2024_cleaned_data_path
        Path in which to save the cleaned Western et al. (2024) data

    annual_mean_dir
        Path in which to save interim annual-mean data

    monthly_mean_dir
        Path in which to save interim monthly-mean data

    seasonality_dir
        Path in which to save interim seasonality data

    inverse_emission_dir
        Path in which to save inverse emissions data

    lat_gradient_dir
        Path in which to save interim latitudinal gradient data

    emissions_split_dir
        Path in which to save the split emissions

    emissions_complete_dir
        Path in which to write the complete emissions scenarios

    magicc_versions_to_run
        MAGICC versions to run

    magicc_root_folder
        Root folder for MAGICC versions

    n_magicc_workers
        Number of MAGICC workers to use when running MAGICC

    magicc_output_dir
        Path in which to write the MAGICC output

    esgf_ready_writing_pool
        Parallel pool to use for writing ESGF-ready files

        If `None`, no parallel processing will be used for this step

    esgf_ready_root_dir
        Path to use as the root for writing ESGF-ready data

    esgf_version
        Version to include in the files for ESGF

    esgf_institution_id
        Institution ID to include in the files for ESGF

    input4mips_cvs_source
        Source from which to get the input4MIPs CVs

    Returns
    -------
    :
        Generated paths

        If any task failed, the future with the failure is returned instead
    """
    ### Used in all flows hence here
    downloaded_cmip7_historical_seasonality_lat_gradient_info = submit_output_aware(
        download_file,
        cmip7_historical_seasonality_lat_gradient_info_raw_file_url,
        out_path=cmip7_historical_seasonality_lat_gradient_info_raw_file,
    )
    cmip7_historical_seasonality_lat_gradient_info_extracted = submit_output_aware(
        extract_tar,
        tar_file=downloaded_cmip7_historical_seasonality_lat_gradient_info,
        extract_root_dir=cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir,
    )
    doi = get_doi.submit()

    ### WMO 2022
    all_wmo_2022_ghgs = {
        "ccl4",
        "cfc11",
        "cfc12",
        "cfc113",
        "cfc114",
        "cfc115",
        "ch3br",
        "ch3ccl3",
        "ch3cl",
        "halon1202",
        "halon1211",
        "halon1301",
        "halon2402",
    }
    wmo_2022_ghgs = tuple(ghg for ghg in ghgs if ghg in all_wmo_2022_ghgs)

    ### Western et al. 2024
    all_western_et_al_2024_ghgs = {
        "hcfc141b",
        "hcfc142b",
        "hcfc22",
    }
    western_et_al_2024_ghgs = tuple(ghg for ghg in ghgs if ghg in all_western_et_al_2024_ghgs)

    ### Gases that require running MAGICC
    magicc_based_ghgs = tuple(
        ghg
        for ghg in ghgs
        if ghg
        in [
            "c2f6",
            "c3f8",
            "c4f10",
            "c5f12",
            "c6f14",
            "c7f16",
            "c8f18",
            "cc4f8",
            "cf4",
            "ch2cl2",
            "ch4",
            "chcl3",
            "co2",
            "hfc125",
            "hfc134a",
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
    )

    ### Equivalence species
    equivalence_ghgs = tuple(
        ghg
        for ghg in ghgs
        if ghg
        in [
            "cfc11eq",
            "cfc12eq",
            "hfc134aeq",
        ]
    )

    unsupported_ghgs = (
        set(ghgs) - set(wmo_2022_ghgs) - set(western_et_al_2024_ghgs) - set(magicc_based_ghgs) - set(equivalence_ghgs)
    )
    if unsupported_ghgs:
        msg = f"The following GHGs are not supported: {unsupported_ghgs}"
        raise AssertionError(msg)

    if magicc_based_ghgs:
        missing_single_projection_gases = {*all_wmo_2022_ghgs, *all_western_et_al_2024_ghgs} - set(ghgs)
        if missing_single_projection_gases:
            cli_args = " ".join(f"--ghg {ghg}" for ghg in missing_single_projection_gases)
            msg = (
                "To generate MAGICC-based projections, all WMO 2022 and Western et al. (2024) GHGs must be provided. "
                f"Missing: {missing_single_projection_gases}. "
                f"CLI args: {cli_args}"
            )
            raise AssertionError(msg)

    ### Get the markers
    scenario_infos_markers = tuple(v for v in scenario_infos if v.cmip_scenario_name is not None)

    create_single_concentration_projection = partial(
        create_scenariomip_ghgs_single_concentration_projection,
        scenario_infos=scenario_infos_markers,
        cmip7_historical_ghg_concentration_source_id=cmip7_historical_ghg_concentration_source_id,
        cmip7_historical_ghg_concentration_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
        cmip7_historical_seasonality_lat_gradient_info_extracted=cmip7_historical_seasonality_lat_gradient_info_extracted,
        annual_mean_dir=annual_mean_dir,
        monthly_mean_dir=monthly_mean_dir,
        seasonality_dir=seasonality_dir,
        inverse_emission_dir=inverse_emission_dir,
        lat_gradient_dir=lat_gradient_dir,
        esgf_ready_root_dir=esgf_ready_root_dir,
        raw_notebooks_root_dir=raw_notebooks_root_dir,
        executed_notebooks_dir=executed_notebooks_dir,
        esgf_ready_writing_pool=esgf_ready_writing_pool,
        esgf_version=esgf_version,
        esgf_institution_id=esgf_institution_id,
        input4mips_cvs_source=input4mips_cvs_source,
        doi=doi,
    )

    wmo_2022_cleaned = submit_output_aware(
        clean_wmo_data, raw_data_path=wmo_raw_data_path, out_file=wmo_cleaned_data_path
    )

    if wmo_2022_ghgs:
        wmo_2022_futures = create_single_concentration_projection(
            ghgs=wmo_2022_ghgs,
            cleaned_data_path=wmo_2022_cleaned,
        )

    western_2024_futures = {}
    if western_et_al_2024_ghgs:
        western_et_al_2024_cleaned = get_western_et_al_2024_clean(
            western_et_al_2024_download_url=western_et_al_2024_download_url,
            western_et_al_2024_raw_tar_file=western_et_al_2024_raw_tar_file,
            western_et_al_2024_extract_path=western_et_al_2024_extract_path,
            western_et_al_2024_extracted_file_of_interest=western_et_al_2024_extracted_file_of_interest,
            out_file=western_et_al_2024_cleaned_data_path,
        )

        for ghg in western_et_al_2024_ghgs:
            western_et_al_2024_extended_ghg = submit_output_aware(
                extend_western_et_al_2024,
                ghg=ghg,
                western_et_al_2024_clean=western_et_al_2024_cleaned,
                out_file=western_et_al_2024_cleaned_data_path.parent / f"western-et-al-2024_{ghg}_extended.feather",
                wmo_2022_clean=wmo_2022_cleaned,
                raw_notebooks_root_dir=raw_notebooks_root_dir,
                executed_notebooks_dir=executed_notebooks_dir,
            )

            western_2024_futures = {
                **western_2024_futures,
                **create_single_concentration_projection(
                    ghgs=[ghg],
                    cleaned_data_path=western_et_al_2024_extended_ghg,
                ),
            }

    magicc_based_futures = {}
    if magicc_based_ghgs:
        inverse_emissions_file = submit_output_aware(
            compile_inverse_emissions,
            in_files=tuple(
                v.inverse_emissions_file_future for v in (*wmo_2022_futures.values(), *western_2024_futures.values())
            ),
            out_file=inverse_emission_dir / "single-concentration-projection_inverse-emissions.feather",
        )

        scenario_files_d = submit_output_aware(
            split_input_emissions_into_individual_files,
            emissions_file=emissions_file,
            scenario_infos=scenario_infos,
            out_dir=emissions_split_dir,
        )

        # Have to wait to ensure that we can use the dictionary in the next step
        scenario_files_d_res = scenario_files_d.result()
        complete_scenario_files_d = {
            scenario_info: submit_output_aware(
                make_complete_scenario,
                scenario_info=scenario_info,
                scenario_file=scenario_file,
                inverse_emissions_file=inverse_emissions_file,
                history_file=scenario_files_d_res["historical"],
                out_file=emissions_complete_dir / f"{scenario_info.to_file_stem()}.feather",
                raw_notebooks_root_dir=raw_notebooks_root_dir,
                executed_notebooks_dir=executed_notebooks_dir,
            )
            for scenario_info, scenario_file in scenario_files_d_res.items()
            if scenario_info != "historical"
        }

        magicc_versions_info_d = {
            version: get_magicc_version_info.submit(version=version, root_folder=magicc_root_folder)
            for version in magicc_versions_to_run
        }
        # Have to block here to get the result for the next step
        magicc_versions_info_d_res = {key: value.result() for key, value in magicc_versions_info_d.items()}

        magicc_output_files_d = {
            (scenario_info, mi.version): submit_output_aware(
                run_magicc,
                scenario_info=scenario_info,
                complete_file=complete_file,
                magicc_version=mi.version,
                magicc_exe=mi.executable,
                magicc_prob_distribution=mi.probabilistic_distribution,
                n_magicc_workers=n_magicc_workers,
                out_file=magicc_output_dir
                / f"{scenario_info.to_file_stem()}_magicc-{mi.version.replace('.', '-')}_results.feather",
                raw_notebooks_root_dir=raw_notebooks_root_dir,
                executed_notebooks_dir=executed_notebooks_dir,
            )
            for scenario_info, complete_file in complete_scenario_files_d.items()
            for mi in magicc_versions_info_d_res.values()
        }
        for v in magicc_output_files_d.values():
            v.wait()

        downloaded_cmip7_historical_ghgs_futures = {
            ghg: submit_output_aware(
                download_cmip7_historical_ghg_concentrations,
                ghg,
                source_id=cmip7_historical_ghg_concentration_source_id,
                root_dir=cmip7_historical_ghg_concentration_data_root_dir,
                checklist_file=cmip7_historical_ghg_concentration_data_root_dir
                / f"{ghg}_{cmip7_historical_ghg_concentration_source_id}.chk",
            )
            for ghg in magicc_based_ghgs
        }

        # For each GHG and marker scenario:
        # - harmonise
        # - get monthly
        # - get future seasonality
        # - get future latitudinal gradient
        # - write ESGF files
        # breakpoint()
        # for ghg, si_marker in zip(magicc_based_ghgs, scenario_infos_markers):
        #     breakpoint()

    if equivalence_ghgs:
        raise NotImplementedError(equivalence_ghgs)

    # Ensure all paths finish
    done, not_done = wait(
        (
            v.esgf_ready_files_future
            for v in (*wmo_2022_futures.values(), *western_2024_futures.values(), *magicc_based_futures.values())
            # Urgh this bloody halon1202 business
            if v.esgf_ready_files_future is not None
        ),
        timeout=30 * 60,
    )
    if not_done:
        raise AssertionError

    res_l = []
    for future in done:
        if future.state != Completed:
            # return the failed future rather than the result
            res_l.append(future)

        else:
            res_l.extend(future.result())

    res = tuple(res_l)

    return res


def create_scenariomip_ghgs(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    emissions_file: Path,
    scenario_infos: tuple[ScenarioInfo, ...],
    run_id: str,
    n_workers: int,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    cmip7_historical_seasonality_lat_gradient_info_raw_file_url: str,
    cmip7_historical_seasonality_lat_gradient_info_raw_file: Path,
    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir: Path,
    wmo_raw_data_path: Path,
    wmo_cleaned_data_path: Path,
    western_et_al_2024_download_url: str,
    western_et_al_2024_raw_tar_file: Path,
    western_et_al_2024_extract_path: Path,
    western_et_al_2024_extracted_file_of_interest: Path,
    western_et_al_2024_cleaned_data_path: Path,
    annual_mean_dir: Path,
    monthly_mean_dir: Path,
    seasonality_dir: Path,
    inverse_emission_dir: Path,
    lat_gradient_dir: Path,
    emissions_split_dir: Path,
    emissions_complete_dir: Path,
    magicc_versions_to_run: tuple[str, ...],
    magicc_root_folder: Path,
    n_magicc_workers: int,
    n_workers_esgf_ready_writing: int,
    magicc_output_dir: Path,
    esgf_ready_root_dir: Path,
    esgf_version: str,
    esgf_institution_id: str,
    input4mips_cvs_source: str,
) -> tuple[Path, ...]:
    """
    Create ScenarioMIP GHGs via a convenience wrapper

    The wrapper enables simpler injection of the run ID into the flow

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

    emissions_file
        File containing emissions received from the harmonisation team

    scenario_infos
        Scenario information

    run_id
        Run ID to use with the [create_scenariomip_ghgs][] flow

    n_workers
        Number of workers to use with parallel processing

    raw_notebooks_root_dir
        Root directory for raw notebooks

    executed_notebooks_dir
        Path in which to write executed notebooks

    cmip7_historical_ghg_concentration_source_id
        Source ID (unique identifier) for historical CMIP7 GHG concentrations

    cmip7_historical_ghg_concentration_data_root_dir
        Root directory for saving CMIP7 historical GHG concentrations

    cmip7_historical_seasonality_lat_gradient_info_raw_file_url
        URL from which to download the CMIP7 historical GHG concentrations seasonality and lat. gradient info

    cmip7_historical_seasonality_lat_gradient_info_raw_file
        File in which to save the CMIP7 historical GHG concentrations seasonality and lat. gradient info

    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir
        Root directory in which to extract `cmip7_historical_seasonality_lat_gradient_info_raw_file`

    wmo_raw_data_path
        Path to the raw WMO data

    wmo_cleaned_data_path
        Path in which to save the extracted WMO data

    western_et_al_2024_download_url
        URL from which to download the raw Western et al. (2024) data

    western_et_al_2024_raw_tar_file
        Path in which to download the raw Western et al. (2024) data

    western_et_al_2024_extract_path
        Path in which to extract the raw Western et al. (2024) data

    western_et_al_2024_extracted_file_of_interest
        File of interest from the extracted Western et al. (2024) data

    western_et_al_2024_cleaned_data_path
        Path in which to save the cleaned Western et al. (2024) data

    annual_mean_dir
        Path in which to save interim annual-mean data

    monthly_mean_dir
        Path in which to save interim monthly-mean data

    seasonality_dir
        Path in which to save interim seasonality data

    inverse_emission_dir
        Path in which to save inverse emissions data

    lat_gradient_dir
        Path in which to save interim latitudinal gradient data

    emissions_split_dir
        Path in which to save the split emissions

    emissions_complete_dir
        Path in which to write the complete emissions scenarios

    magicc_versions_to_run
        MAGICC versions to run

    magicc_root_folder
        Root folder for MAGICC versions

    n_magicc_workers
        Number of MAGICC workers to use when running MAGICC

    n_workers_esgf_ready_writing
        Number of workers to use when writing ESGF-ready files

    magicc_output_dir
        Path in which to write the MAGICC output

    esgf_ready_root_dir
        Path to use as the root for writing ESGF-ready data

    esgf_version
        Version to include in the files for ESGF

    esgf_institution_id
        Institution ID to include in the files for ESGF

    input4mips_cvs_source
        Source from which to get the input4MIPs CVs

    Returns
    -------
    :
        Generated paths
    """
    # A note on this. I tried with the dask runner.
    # It didn't really behave how I wanted it to.
    # It seemed to not spin up and shut down workers properly
    # (sometimes the workers would get killed before the job was actually finished).
    # So, I got rid of that and now just use the task runner.
    # This uses threads, which doesn't work for all tasks.
    # As a result, some tasks are given a process pool
    # to essentially introduce parallel processing by hand
    # while also not blocking task submission or causing crashes
    # (and it can be a bit of trial and error to figure out
    # which tasks need their own multiprocess pool and which don't).
    # It's a bit of a hack and fiddly to get the multiprocess pool stuff working
    # without blocking in the wrong places,
    # but doing it this way seems to give more stable, predictable behaviour
    # (and now we have implementations to follow,
    # the pattern is pretty easy to repeat).
    if n_workers == 1:
        task_runner = ThreadPoolTaskRunner(max_workers=1)

    else:
        task_runner = ThreadPoolTaskRunner(max_workers=n_workers)

    # A bit of trickery here to inject the run ID dynamically
    run_id_flow = flow(
        name=f"scenariomip-ghgs_{run_id}",
        task_runner=task_runner,
    )(create_scenariomip_ghgs_flow)

    perw = (
        multiprocessing.Pool(processes=n_workers_esgf_ready_writing)
        if n_workers_esgf_ready_writing > 1
        else nullcontext()
    )

    with perw as pool_esgf_ready_writing:
        res_flow = run_id_flow(
            ghgs=ghgs,
            emissions_file=emissions_file,
            scenario_infos=scenario_infos,
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
            n_magicc_workers=n_magicc_workers,
            magicc_output_dir=magicc_output_dir,
            esgf_ready_writing_pool=pool_esgf_ready_writing,
            esgf_ready_root_dir=esgf_ready_root_dir,
            esgf_version=esgf_version,
            esgf_institution_id=esgf_institution_id,
            input4mips_cvs_source=input4mips_cvs_source,
        )

    return res_flow
