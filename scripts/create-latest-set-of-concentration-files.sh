#!/bin/bash
# Generate our latest version of the concentration files.
#
# May not work on windows as this is a shell script,
# but the commands should be easy to copy-paste
# (if they're not, we've made the script too complex).
pixi run python scripts/generate-concentration-files.py \
    --run-id "1.0.0" \
    --esgf-version 1.0.0 \
    --input4mips-cvs-source "gh:ghg-concs-vl-final" \
    --n-workers 2 \
    --n-workers-multiprocessing 8 \
    --n-workers-multiprocessing-magicc 2 \
    --n-workers-per-magicc-notebook 6 \
    --emissions-file data/raw/input-scenarios/202512021030_202512071232_202511040855_202511040855_complete-emissions.csv
