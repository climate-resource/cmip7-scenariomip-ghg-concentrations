#!/bin/bash
# Generate our latest version of the concentration files.
#
# May not work on windows as this is a shell script,
# but the commands should be easy to copy-paste
# (if they're not, we've made the script too complex).
RUN_ID="1.1.0"
# RUN_ID="dev-test"

INPUT4MIPS_CVS_SOURCE="gh:ghg-vl-cf"

ESGF_VERSION="1.1.0"

# Use this to refresh the cache for all tasks
# PREFECT_TASKS_REFRESH_CACHE=true

# echo "============================"
# echo "Generating ESGF-ready files"
# echo "============================"
# pixi run prefect profile use cmip7-scenariomip-ghg-concentrations
# pixi run python scripts/generate-concentration-files.py \
#     --run-id "${RUN_ID}" \
#     --esgf-version "${ESGF_VERSION}" \
#     --input4mips-cvs-source "${INPUT4MIPS_CVS_SOURCE}" \
#     --n-workers 2 \
#     --n-workers-multiprocessing 8 \
#     --n-workers-multiprocessing-magicc 2 \
#     --n-workers-per-magicc-notebook 6 \
#     --emissions-file data/raw/input-scenarios/202603251220_202512071232_202511040855_202511040855_complete-emissions.csv \
#     --scenario vl \
#     --scenario ln \
#     --scenario l \
#     --scenario ml \
#     --scenario m \
#     --scenario hl \
#     --scenario h
#
# generate_exit_code=$?
generate_exit_code=0

if [ $"$generate_exit_code" -eq 0 ]; then
    echo "Produced files for ScenarioMIP"

    n_files_produced=$(find "output-bundles/${RUN_ID}/data/processed/esgf-ready/input4MIPs/CMIP7/ScenarioMIP" -type f | wc -l)
    echo "Number of ScenarioMIP files produced: ${n_files_produced}"
    if [ "${n_files_produced}" -eq 8050 ]; then
        echo "Looks good"
    else
        echo "Expected number of ScenarioMIP files: 8050 (46 species, 7 scenarios, 5 different resolution-frequency combinations, 5 different time slices). Something wrong?"
        exit 1
    fi

else
    echo "Generating ScenarioMIP files failed"
    exit $generate_exit_code

fi

# Developer note: this is a hack.
# We have to have the vl files
# in order to make the vl-cf files.
# You could do this by fixing the workflow
# (and that's what we should do long-term).

# Urgh
export VL_CACHE_HACK=True
echo "====================================="
echo "Generating ESGF-ready files for vl-cf"
echo "====================================="
pixi run prefect profile use cmip7-scenariomip-ghg-concentrations
pixi run python scripts/generate-concentration-files.py \
    --run-id "${RUN_ID}" \
    --esgf-version "${ESGF_VERSION}" \
    --n-workers 2 \
    --n-workers-multiprocessing 8 \
    --n-workers-multiprocessing-magicc 2 \
    --n-workers-per-magicc-notebook 6 \
    --input4mips-cvs-source "${INPUT4MIPS_CVS_SOURCE}" \
    --harmonisation-year 2015 \
    --esgf-files-start-year 2016 \
    --emissions-file data/raw/input-scenarios/202607141103_markers_lixo_202512071232_202607200001_202607200001_complete-emissions.csv \
    --scenario vl-cf

# --n-workers 1 \
# --n-workers-multiprocessing 1 \
# --n-workers-multiprocessing-magicc 1 \
# --n-workers-per-magicc-notebook 6 \

generate_exit_code=$?

if [ $"$generate_exit_code" -eq 0 ]; then
    n_files_produced=$(find "output-bundles/${RUN_ID}/data/processed/esgf-ready/input4MIPs/CMIP7/PolMIP" -type f | wc -l)
    echo "Number of PolMIP files produced: ${n_files_produced}"
    if [ "${n_files_produced}" -eq 1150 ]; then
        echo "Looks good"
    else
        echo "Expected number of PolMIP files: 1150 (46 species, 1 scenario, 5 different resolution-frequency combinations, 5 different time slices). Something wrong?"
        exit 1
    fi

else
    echo "Generating PolMIP files failed"
    exit $generate_exit_code

fi
