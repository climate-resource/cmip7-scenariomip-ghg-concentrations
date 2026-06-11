"""
Extract MAGICC results for SLEIP
"""

from pathlib import Path

import pandas_indexing as pix
import pandas_openscm
import pandas_openscm.db
import typer
from gcages.ar6 import AR6PostProcessor


def main() -> None:
    """
    Extract the results
    """
    # out_file = "20261206_magicc-for-sleip.csv"
    out_file = "20261206_magicc-for-sleip.parquet.gzip"

    db_backend_str: str = "feather"
    db_dir = Path("output-bundles/1.1.0/data/interim/magicc-output/db")
    db = pandas_openscm.db.OpenSCMDB(
        backend_data=pandas_openscm.db.DATA_BACKENDS.get_instance(db_backend_str),
        backend_index=pandas_openscm.db.INDEX_BACKENDS.get_instance(db_backend_str),
        db_dir=db_dir,
    )

    tmp = db.load(
        pix.isin(
            variable=["Surface Air Temperature Change", "Heat Content|Ocean"],
            run_mode="magicc-concentration-to-emissions-switch",
        )
    ).reset_index(["run_mode"], drop=True)

    post_processor = AR6PostProcessor.from_ar6_config(n_processes=None)
    post_processed_results = post_processor(tmp)

    out = pix.concat([tmp.loc[pix.isin(variable="Heat Content|Ocean")], post_processed_results.timeseries_run_id])

    # out.to_csv(out_file)
    out.to_parquet(out_file, compression="gzip")


if __name__ == "__main__":
    typer.run(main)
