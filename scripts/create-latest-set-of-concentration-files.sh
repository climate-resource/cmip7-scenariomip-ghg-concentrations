#!/bin/bash
# Generate our latest version of the concentration files.
#
# May not work on windows as this is a shell script,
# but the commands should be easy to copy-paste
# (if they're not, we've made the script too complex).
RUN_ID="1.1.0"
RUN_ID="dev-test"

INPUT4MIPS_CVS_SOURCE="gh:ghg-vl-cf"

# Use this to refresh the cache for all tasks
# PREFECT_TASKS_REFRESH_CACHE=true

echo "============================"
echo "Generating ESGF-ready files"
echo "============================"
pixi run prefect profile use cmip7-scenariomip-ghg-concentrations
pixi run python scripts/generate-concentration-files.py \
    --run-id "${RUN_ID}" \
    --esgf-version 1.1.0 \
    --input4mips-cvs-source "${INPUT4MIPS_CVS_SOURCE}" \
    --n-workers 2 \
    --n-workers-multiprocessing 8 \
    --n-workers-multiprocessing-magicc 2 \
    --n-workers-per-magicc-notebook 6 \
    --emissions-file data/raw/input-scenarios/202603251220_202512071232_202511040855_202511040855_complete-emissions.csv \
    --scenario vl \
    --scenario h
# --scenario ln \
# --scenario l \
# --scenario ml \
# --scenario m \
# --scenario hl \

# Developer note: this is a hack.
# We have to have the vl files
# in order to make the vl-cf files.
# You could do this by fixing the workflow
# (and that's what we should do long-term).
echo "====================================="
echo "Generating ESGF-ready files for vl-cf"
echo "====================================="
pixi run prefect profile use cmip7-scenariomip-ghg-concentrations
pixi run python scripts/generate-concentration-files.py \
    --run-id "${RUN_ID}" \
    --esgf-version 1.1.0 \
    --input4mips-cvs-source "${INPUT4MIPS_CVS_SOURCE}" \
    --n-workers 2 \
    --n-workers-multiprocessing 8 \
    --n-workers-multiprocessing-magicc 2 \
    --n-workers-per-magicc-notebook 6 \
    --harmonisation-year 2015 \
    --esgf-files-start-year 2015 \
    --emissions-file data/raw/input-scenarios/202607141103_markers_lixo_202512071232_202607200001_202607200001_complete-emissions.csv \
    --scenario vl-cf

# generate_exit_code=$?
#
# if [ $"$generate_exit_code" -eq 0 ]; then
#     n_files_produced=$(find "output-bundles/${RUN_ID}/data/processed/esgf-ready/input4MIPs" -type f | wc -l)
#     echo "Number of files produced: ${n_files_produced}"
#     if [ "${n_files_produced}" -eq 9200 ]; then
#         echo "Looks good"
#     else
#         echo "Expected number of files: 9200 (46 species, 8 scenarios, 5 different resolution-frequency combinations, 5 different time slices). Something wrong?"
#         exit 1
#     fi
#
# else
#     echo "Generating files failed"
#     exit $generate_exit_code
#
# fi
