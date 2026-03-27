#!/bin/bash
# Generate our latest version of the concentration files.
#
# May not work on windows as this is a shell script,
# but the commands should be easy to copy-paste
# (if they're not, we've made the script too complex).
RUN_ID="1.1.0"

echo "============================"
echo "Generating ESGF-ready files"
echo "============================"
# Use this to refresh the cache for all tasks
# PREFECT_TASKS_REFRESH_CACHE=true
pixi run prefect profile use cmip7-scenariomip-ghg-concentrations
pixi run python scripts/generate-concentration-files.py \
    --run-id "${RUN_ID}" \
    --esgf-version 1.1.0 \
    --input4mips-cvs-source "gh:ghg-concs-v1-1-0" \
    --n-workers 2 \
    --n-workers-multiprocessing 8 \
    --n-workers-multiprocessing-magicc 2 \
    --n-workers-per-magicc-notebook 6 \
    --emissions-file data/raw/input-scenarios/202603251220_202512071232_202511040855_202511040855_complete-emissions.csv
# --run-id "dev-test"

generate_exit_code=$?

if [ $"$generate_exit_code" -eq 0 ]; then
    n_files_produced=$(find "output-bundles/${RUN_ID}/data/processed/esgf-ready/input4MIPs" -type f | wc -l)
    echo "Number of files produced: ${n_files_produced}"
    if [ "${n_files_produced}" -eq 8050 ]; then
        echo "Looks good"
    else
        echo "Expected number of files: 8050 (46 species, 7 scenarios, 5 different resolution-frequency combinations, 5 different time slices). Something wrong?"
        exit 1
    fi

else
    echo "Generating files failed"
    exit $generate_exit_code

fi
