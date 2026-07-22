"""
Compare input emissions to what is on Zenodo

https://zenodo.org/records/18497404
"""

from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_indexing as pix
from pandas_openscm import register_pandas_accessor
from pandas_openscm.comparison import compare_close
from pandas_openscm.io import load_timeseries_csv


def main():
    """Run the comparison"""
    register_pandas_accessor()

    # Get from https://zenodo.org/records/18497404
    zenodo_p = Path("ScenarioMIP_emissions_marker_scenarios_v0.1.xlsx")
    local_p = Path(
        # "data/raw/input-scenarios/202601301330_202512071232_202511040855_202511040855_complete-emissions.csv"
        "data/raw/input-scenarios/202603251220_202512071232_202511040855_202511040855_complete-emissions.csv"
    )

    zenodo = pd.read_excel("ScenarioMIP_emissions_marker_scenarios_v0.1.xlsx", sheet_name="data")
    zenodo = zenodo.set_index(["model", "scenario", "region", "variable", "unit"])
    zenodo.columns = zenodo.columns.astype(int)
    zenodo.columns.name = "year"
    zenodo = zenodo.openscm.update_index_levels(
        {"variable": lambda x: x.replace("Climate Assessment|Harmonized and Infilled|", "")}
    )
    zenodo = zenodo.openscm.update_index_levels(
        {
            # "variable": lambda x: x.replace("HFC43-10", "HFC4310mee"),
            "unit": lambda x: x.replace("HFC4310mee", "HFC4310"),
        }
    )
    zenodo = zenodo.loc[pix.ismatch(variable="Emissions**")]

    new = load_timeseries_csv(
        local_p,
        index_columns=["model", "scenario", "variable", "region", "unit"],
        out_columns_type=int,
        out_columns_name="year",
    )

    zenodo = zenodo.loc[pix.isin(variable=new.pix.unique("variable"))]
    zenodo = zenodo.dropna(how="all", axis="columns")

    for (model, scenario), msdf_zenodo in zenodo.groupby(["model", "scenario"]):
        exp_n_rows = 52
        if msdf_zenodo.shape[0] != exp_n_rows:
            raise AssertionError

        msdf_new = new.openscm.mi_loc(pd.MultiIndex.from_tuples([(model,)], names=["model"]))
        # msdf_new.loc[pix.ismatch(variable="**43**")]
        # msdf_zenodo.loc[pix.ismatch(variable="**43**")]

        overlapping_cols = np.intersect1d(msdf_zenodo.columns, msdf_new.columns)
        msdf_zenodo_l = msdf_zenodo.loc[:, overlapping_cols].reset_index("scenario", drop=True)
        msdf_new = msdf_new.loc[:, overlapping_cols].reset_index("scenario", drop=True)

        diffs = compare_close(
            left=msdf_zenodo_l,
            left_name=zenodo_p.name,
            right=msdf_new,
            right_name=local_p.name,
            isclose=partial(np.isclose, rtol=1e-8, atol=1e-8),
        )
        if not diffs.empty:
            print(f"Diffs for {model} {scenario}")
            print()
            print("Variables affected:")
            print(
                diffs.index.droplevel(diffs.index.names.difference(["variable", "region"]))
                .drop_duplicates()
                .to_frame(index=False)
            )
            print()
            print("Full diffs")
            print(diffs)
            print()

        else:
            print(f"No difference for {model} {scenario}")
            print()


if __name__ == "__main__":
    main()
