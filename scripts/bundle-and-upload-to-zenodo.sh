#!/bin/bash
# Bundle our outputs and upload to Zenodo
#
# May not work on windows as this is a shell script,
# but the commands should be easy to copy-paste
# (if they're not, we've made the script too complex).
RUN_ID="1.1.0"

echo "============================"
echo "Creating zenodo-ready bundle"
echo "============================"
pixi run pixi run python scripts/create-zenodo-ready-bundle.py "output-bundles/${RUN_ID}"

create_bundle_rc=$?

if [ "$create_bundle_rc" -ne 0 ]; then
    echo "Creating zenodo-ready bundle failed"
    exit $create_bundle_rc
fi

echo "==================="
echo "Uploading to zenodo"
echo "==================="
pixi run pixi run python scripts/upload-to-zenodo.py "zenodo-bundles/${RUN_ID}"
