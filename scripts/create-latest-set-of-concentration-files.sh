#!/bin/bash
# Generate our latest version of the concentration files.
#
# May not work on windows as this is a shell script,
# but the commands should be easy to copy-paste
# (if they're not, we've made the script too complex).
RUN_ID="1.0.1"

echo "============================"
echo "Generating ESGF-ready files"
echo "============================"
# Use this to refresh the cache for all tasks
# PREFECT_TASKS_REFRESH_CACHE=true
pixi run prefect profile use cmip7-scenariomip-ghg-concentrations
pixi run python scripts/generate-concentration-files.py \
    --run-id "${RUN_ID}" \
    --esgf-version 1.0.1 \
    --input4mips-cvs-source "gh:ghg-concs-lower-priority" \
    --n-workers 2 \
    --n-workers-multiprocessing 8 \
    --n-workers-multiprocessing-magicc 2 \
    --n-workers-per-magicc-notebook 6 \
    --emissions-file tbd.csv
# --emissions-file data/raw/input-scenarios/202603081555_202512071232_202511040855_202511040855_complete-emissions.csv
# --run-id "dev-test"
