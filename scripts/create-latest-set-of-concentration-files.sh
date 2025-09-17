#!/bin/bash
# Generate our latest version of the concentration files.
#
# May not work on windows as this is a shell script,
# but the commands should be easy to copy-paste
# (if they're not, we've made the script too complex).
pixi run python scripts/generate-concentration-files.py --esgf-version 0.1.0 --input4mips-cvs-source "gh:6d444ea00177d9988ed93bc4cddce12829307367" --emissions-file data/raw/input-scenarios/20250805-zn-scratch_0003_0003_0002_harmonised-emissions-up-to-sillicone.csv
