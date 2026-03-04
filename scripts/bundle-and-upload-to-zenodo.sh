#!/bin/bash
# Bundle our outputs and upload to Zenodo
#
# May not work on windows as this is a shell script,
# but the commands should be easy to copy-paste
# (if they're not, we've made the script too complex).
RUN_ID="1.0.1"

echo "============================"
echo "Creating zenodo-ready bundle"
echo "============================"
pixi run pixi run python scripts/create-zenodo-ready-bundle.py "output-bundles/${RUN_ID}"

echo "==================="
echo "Uploading to zenodo"
echo "==================="
pixi run pixi run python scripts/upload-to-zenodo.py "zenodo-bundles/${RUN_ID}"
