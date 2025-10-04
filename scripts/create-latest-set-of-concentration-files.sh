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
    --emissions-file data/raw/input-scenarios/20250805-zn-scratch_0003_0003_0002_harmonised-emissions-up-to-sillicone.csv
