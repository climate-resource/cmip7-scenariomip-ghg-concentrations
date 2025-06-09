"""
Main flow
"""

import itertools
import multiprocessing.pool
from contextlib import nullcontext
from functools import partial
from pathlib import Path

from prefect import flow
from prefect.futures import PrefectFuture, wait
from prefect.states import Completed
from prefect.task_runners import ThreadPoolTaskRunner

from cmip7_scenariomip_ghg_generation.constants import EQUIVALENT_SPECIES_COMPONENTS
from cmip7_scenariomip_ghg_generation.prefect_helpers import submit_output_aware
from cmip7_scenariomip_ghg_generation.prefect_tasks import (
    clean_wmo_data,
    compile_inverse_emissions,
    create_esgf_files,
    create_esgf_files_equivalence_species,
    create_gradient_aware_harmonisation_annual_mean_file,
    create_one_box_annual_mean_file,
    download_cmip7_historical_ghg_concentrations,
    download_file,
    extend_western_et_al_2024,
    extract_specific_variable_from_collection,
    extract_tar,
    get_doi,
    get_magicc_version_info,
    get_western_et_al_2024_clean,
    interpolate_annual_mean_to_monthly,
    make_complete_scenario,
    plot_marker_overview,
    run_magicc,
    scale_lat_gradient_based_on_emissions,
    scale_seasonality_based_on_annual_mean,
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
    magicc_output_db_dir: Path,
    magicc_db_backend_str: str,
    magicc_based_ghgs_projection_method: dict[str, str],
    single_variable_dir: Path,
    plot_complete_dir: Path,
    esgf_ready_root_dir: Path,
    esgf_version: str,
    esgf_institution_id: str,
    input4mips_cvs_source: str,
    pool_multiprocessing: multiprocessing.pool.Pool | None,
    pool_multiprocessing_magicc: multiprocessing.pool.Pool | None,
    n_workers_per_magicc_notebook: int,
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

    magicc_output_db_dir
        Directory for the MAGICC output database

    magicc_db_backend_str
        Name of the back-end to use for the MAGICC output database

    magicc_based_ghgs_projection_method
        Projection method to use for MAGICC-based GHGs

        The point here is that for some gases,
        we simply use a one-box model
        instead of MAGICC because it's simpler and easier to harmonise.

    single_variable_dir
        Directory in which to write single variable files as needed

    plot_complete_dir
        Directory in which to write complete files for plotting

    esgf_ready_root_dir
        Path to use as the root for writing ESGF-ready data

    esgf_version
        Version to include in the files for ESGF

    esgf_institution_id
        Institution ID to include in the files for ESGF

    input4mips_cvs_source
        Source from which to get the input4MIPs CVs

    pool_multiprocessing
        Parallel pool to use for multiprocessing

        If `None`, no parallel processing will be used

    pool_multiprocessing_magicc
        Parallel pool to use for multiprocessing of MAGICC-related steps

        If `None`, no parallel processing will be used

    n_workers_per_magicc_notebook
        Number of MAGICC workers to use in each MAGICC-related step/notebook

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

    if equivalence_ghgs:
        for eg in equivalence_ghgs:
            equivalence_ghg_components = EQUIVALENT_SPECIES_COMPONENTS[eg]
            missing_gases = set(equivalence_ghg_components) - set(ghgs)
            if missing_gases:
                cli_args = " ".join(f"--ghg {ghg}" for ghg in missing_gases)
                msg = (
                    f"To generate {eg}, you need multiple components. "
                    f"Missing: {missing_gases}. "
                    f"CLI args: {cli_args}"
                )
                raise AssertionError(msg)

    ### Get the markers
    scenario_info_markers = tuple(v for v in scenario_infos if v.cmip_scenario_name is not None)

    create_single_concentration_projection = partial(
        create_scenariomip_ghgs_single_concentration_projection,
        scenario_infos=scenario_info_markers,
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
        esgf_version=esgf_version,
        esgf_institution_id=esgf_institution_id,
        input4mips_cvs_source=input4mips_cvs_source,
        doi=doi,
        pool_multiprocessing=pool_multiprocessing,
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

        # Have to get the result to ensure that we can use the dictionary in the next step
        # (yes, blocking, but I can't see how to easily avoid this and the time penalty isn't so high)
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
                pool=pool_multiprocessing,
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

        magicc_complete_files_d = {
            (scenario_info, mi.version): submit_output_aware(
                run_magicc,
                scenario_info=scenario_info,
                complete_file=complete_file,
                magicc_version=mi.version,
                magicc_exe=mi.executable,
                magicc_prob_distribution=mi.probabilistic_distribution,
                db_dir=magicc_output_db_dir,
                db_backend_str=magicc_db_backend_str,
                out_file=magicc_output_db_dir.parent
                / f"{scenario_info.to_file_stem()}_magicc-{mi.version.replace('.', '-')}_results.complete",
                raw_notebooks_root_dir=raw_notebooks_root_dir,
                executed_notebooks_dir=executed_notebooks_dir,
                n_magicc_workers=n_workers_per_magicc_notebook,
                pool=pool_multiprocessing_magicc,
            )
            for scenario_info, complete_file in complete_scenario_files_d.items()
            for mi in magicc_versions_info_d_res.values()
        }

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

        complete_scenario_files_markers = tuple(
            file for si, file in complete_scenario_files_d.items() if si.cmip_scenario_name is not None
        )
        magicc_v760a3_complete_files_markers = tuple(
            res
            for (si, magicc_version), res in magicc_complete_files_d.items()
            if si.cmip_scenario_name is not None and magicc_version == "MAGICCv7.6.0a3"
        )
        tmp = []
        for ghg in magicc_based_ghgs:
            global_mean_yearly_common_kwargs = dict(
                ghg=ghg,
                scenario_info_markers=scenario_info_markers,
                historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
                magicc_output_db_dir=magicc_output_db_dir,
                magicc_db_backend_str=magicc_db_backend_str,
                raw_notebooks_root_dir=raw_notebooks_root_dir,
                executed_notebooks_dir=executed_notebooks_dir,
                pool=pool_multiprocessing,
                wait_for=[downloaded_cmip7_historical_ghgs_futures[ghg], *magicc_v760a3_complete_files_markers],
            )
            if ghg in ["co2", "ch4", "n2o"]:
                if magicc_based_ghgs_projection_method[ghg] == "gradient-aware-harmonisation":
                    global_mean_yearly_file_future = submit_output_aware(
                        create_gradient_aware_harmonisation_annual_mean_file,
                        out_file=annual_mean_dir / f"gradient-aware-harmonisation_{ghg}_annual-mean.feather",
                        **global_mean_yearly_common_kwargs,
                    )

                else:
                    raise NotImplementedError(magicc_based_ghgs_projection_method[ghg])

            else:
                # Submit in all cases, even if not used,
                # so we have a record of how much the choice between MAGICC
                # and one-box matters.
                one_box_annual_mean_file = submit_output_aware(
                    create_one_box_annual_mean_file,
                    emissions_complete_dir=emissions_complete_dir,
                    out_file=annual_mean_dir / f"one-box_{ghg}_annual-mean.feather",
                    **global_mean_yearly_common_kwargs,
                )
                if magicc_based_ghgs_projection_method[ghg] == "gradient-aware-harmonisation":
                    global_mean_yearly_file_future = submit_output_aware(
                        create_gradient_aware_harmonisation_annual_mean_file,
                        out_file=annual_mean_dir / f"gradient-aware-harmonisation_{ghg}_annual-mean.feather",
                        **global_mean_yearly_common_kwargs,
                    )

                elif magicc_based_ghgs_projection_method[ghg] == "one-box":
                    global_mean_yearly_file_future = one_box_annual_mean_file

                else:
                    raise NotImplementedError(magicc_based_ghgs_projection_method[ghg])

            global_mean_monthly_file_future = submit_output_aware(
                interpolate_annual_mean_to_monthly,
                ghg=ghg,
                annual_mean_file=global_mean_yearly_file_future,
                historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
                historical_data_seasonality_lat_gradient_info_root=(
                    cmip7_historical_seasonality_lat_gradient_info_extracted
                ),
                out_file=monthly_mean_dir / f"modelling-based-projection_{ghg}_monthly-mean.nc",
                raw_notebooks_root_dir=raw_notebooks_root_dir,
                executed_notebooks_dir=executed_notebooks_dir,
            )

            if ghg in ["co2"]:
                # Scale seasonality based on NPP
                # (will require redoing to the regression)
                print(f"skipping {ghg}")

            else:
                seasonality_all_time_file_future = submit_output_aware(
                    scale_seasonality_based_on_annual_mean,
                    ghg=ghg,
                    annual_mean_file=global_mean_yearly_file_future,
                    historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
                    historical_data_seasonality_lat_gradient_info_root=(
                        cmip7_historical_seasonality_lat_gradient_info_extracted
                    ),
                    out_file=seasonality_dir / f"modelling-based-projection_{ghg}_seasonality-all-time.nc",
                    raw_notebooks_root_dir=raw_notebooks_root_dir,
                    executed_notebooks_dir=executed_notebooks_dir,
                )

            if ghg in ["co2", "ch4", "n2o"]:
                # Scale latitudinal gradient using
                # fossil emissions for co2 and ch4,
                # total emissions for n2o.
                # Only first PC changes.
                # Second PC is assumed constant in future.
                print(f"skipping {ghg}")
                continue

            else:
                ghg_annual_mean_emissions_file = extract_specific_variable_from_collection.submit(
                    extract_from=complete_scenario_files_markers,
                    scenario_infos=scenario_info_markers,
                    # Scale latitudinal gradient using total emissions
                    variable_lower=f"emissions|{ghg}",
                    out_file=single_variable_dir / f"{ghg}_total.feather",
                )

                lat_gradient_file_future = submit_output_aware(
                    scale_lat_gradient_based_on_emissions,
                    ghg=ghg,
                    annual_mean_emissions_file=ghg_annual_mean_emissions_file,
                    historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
                    historical_data_seasonality_lat_gradient_info_root=(
                        cmip7_historical_seasonality_lat_gradient_info_extracted
                    ),
                    out_file=lat_gradient_dir / f"{ghg}_latitudinal-gradient-info.nc",
                    raw_notebooks_root_dir=raw_notebooks_root_dir,
                    executed_notebooks_dir=executed_notebooks_dir,
                )

            esgf_ready_futures = {
                (ghg, si.cmip_scenario_name): submit_output_aware(
                    create_esgf_files,
                    ghg=ghg,
                    cmip_scenario_name=si.cmip_scenario_name,
                    internal_processing_scenario_name=si.cmip_scenario_name,
                    esgf_version=esgf_version,
                    esgf_institution_id=esgf_institution_id,
                    input4mips_cvs_source=input4mips_cvs_source,
                    doi=doi,
                    global_mean_monthly_file=global_mean_monthly_file_future,
                    seasonality_file=seasonality_all_time_file_future,
                    lat_gradient_file=lat_gradient_file_future,
                    esgf_ready_root_dir=esgf_ready_root_dir,
                    raw_notebooks_root_dir=raw_notebooks_root_dir,
                    executed_notebooks_dir=executed_notebooks_dir,
                    checklist_file=esgf_ready_root_dir / f"{ghg}_{si.cmip_scenario_name}.chk",
                    pool=pool_multiprocessing,
                )
                for si in scenario_info_markers
            }

            tmp.extend(esgf_ready_futures.values())

        # TODO: remove this and use magicc_based_futures
        for v in tmp:
            v.wait()

    if equivalence_ghgs:
        equivalence_ghgs_esgf_ready_futures = {
            (equivalent_species, si.cmip_scenario_name): submit_output_aware(
                # TODO: fix up flow so we don't need to duplicate the file-writing logic so much
                create_esgf_files_equivalence_species,
                equivalent_species=equivalent_species,
                components=EQUIVALENT_SPECIES_COMPONENTS[equivalent_species],
                cmip_scenario_name=si.cmip_scenario_name,
                input4mips_cvs_source=input4mips_cvs_source,
                esgf_ready_root_dir=esgf_ready_root_dir,
                raw_notebooks_root_dir=raw_notebooks_root_dir,
                executed_notebooks_dir=executed_notebooks_dir,
                checklist_file=esgf_ready_root_dir / f"{equivalent_species}_{si.cmip_scenario_name}.chk",
                pool=pool_multiprocessing,
            )
            for equivalent_species, si in itertools.product(equivalence_ghgs, scenario_info_markers)
        }
        # TODO: remove this and use equivalence_futures and done at the end
        for v in equivalence_ghgs_esgf_ready_futures.values():
            v.wait()

    # TODO: turn this back on
    # if magicc_based_ghgs:
    if False:
        plotting_futures_l = []
        plotting_futures_l.append(
            plot_marker_overview.submit(
                scenario_info_markers=scenario_info_markers,
                emissions_complete_dir=emissions_complete_dir,
                magicc_output_db_dir=magicc_output_db_dir,
                magicc_db_backend_str=magicc_db_backend_str,
                dependency_complete_files=tuple(
                    v for k, v in magicc_complete_files_d.items() if k[0].cmip_scenario_name is not None
                ),
                complete_file=plot_complete_dir / "plot-marker-overview.complete",
                raw_notebooks_root_dir=raw_notebooks_root_dir,
                executed_notebooks_dir=executed_notebooks_dir,
            )
        )
        for pt in plotting_futures_l:
            pt.wait()

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
    magicc_output_db_dir: Path,
    magicc_db_backend_str: str,
    magicc_based_ghgs_projection_method: dict[str, str],
    single_variable_dir: Path,
    esgf_ready_root_dir: Path,
    esgf_version: str,
    esgf_institution_id: str,
    input4mips_cvs_source: str,
    n_workers: int,
    n_workers_multiprocessing: int,
    n_workers_multiprocessing_magicc: int,
    n_workers_per_magicc_notebook: int,
    plot_complete_dir: Path,
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

    magicc_output_db_dir
        Directory for the MAGICC output database

    magicc_db_backend_str
        Name of the back-end to use for the MAGICC output database

    magicc_based_ghgs_projection_method
        Projection method to use for MAGICC-based GHGs

        The point here is that for some gases,
        we simply use a one-box model
        instead of MAGICC because it's simpler and easier to harmonise.

    single_variable_dir
        Directory in which to write single variable files as needed

    plot_complete_dir
        Directory in which to write complete files for plotting

    esgf_ready_root_dir
        Path to use as the root for writing ESGF-ready data

    esgf_version
        Version to include in the files for ESGF

    esgf_institution_id
        Institution ID to include in the files for ESGF

    input4mips_cvs_source
        Source from which to get the input4MIPs CVs

    n_workers
        Number of (threaded) task runners to use

    n_workers_multiprocessing
        Number of multiprocessing workers to use for tasks that can be run in parallel

    n_workers_multiprocessing_magicc
        Number of multiprocessing workers to use for MAGICC-running tasks

    n_workers_per_magicc_notebook
        Number of MAGICC workers to use per MAGICC-running task

    Returns
    -------
    :
        Generated paths
    """
    ### A note on parallelisation
    #
    # I tried with the dask runner.
    # It didn't really behave how I wanted it to.
    # It seemed to not spin up and shut down workers properly
    # (sometimes the workers would get killed before the job was actually finished).
    #
    # So, I got rid of the dask runner and now just use the threaded task runner.
    # This uses threads, which doesn't work for all tasks.
    # As a result, some tasks are given a process pool
    # to essentially introduce parallel processing by hand
    # while also not blocking task submission or causing crashes
    # (it can be a bit of trial and error to figure out
    # which tasks need their own multiprocess pool and which don't).
    #
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

    potential_multiprocessing_pool = (
        multiprocessing.Pool(processes=n_workers_multiprocessing) if n_workers_multiprocessing > 1 else nullcontext()
    )
    potential_multiprocessing_pool_magicc = (
        multiprocessing.Pool(processes=n_workers_multiprocessing_magicc)
        if n_workers_multiprocessing_magicc > 1
        else nullcontext()
    )

    with (
        potential_multiprocessing_pool as pool_multiprocessing,
        potential_multiprocessing_pool_magicc as pool_multiprocessing_magicc,
    ):
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
            magicc_output_db_dir=magicc_output_db_dir,
            magicc_db_backend_str=magicc_db_backend_str,
            magicc_based_ghgs_projection_method=magicc_based_ghgs_projection_method,
            single_variable_dir=single_variable_dir,
            plot_complete_dir=plot_complete_dir,
            esgf_ready_root_dir=esgf_ready_root_dir,
            esgf_version=esgf_version,
            esgf_institution_id=esgf_institution_id,
            input4mips_cvs_source=input4mips_cvs_source,
            pool_multiprocessing=pool_multiprocessing,
            pool_multiprocessing_magicc=pool_multiprocessing_magicc,
            n_workers_per_magicc_notebook=n_workers_per_magicc_notebook,
        )

    return res_flow
