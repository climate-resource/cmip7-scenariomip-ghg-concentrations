from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
from pandas_openscm import register_pandas_accessor
from pandas_openscm.comparison import compare_close
from pandas_openscm.io import load_timeseries_csv


def main():
    register_pandas_accessor()

    base_p = Path("data/raw/input-scenarios/202601301330_202512071232_202511040855_202511040855_complete-emissions.csv")
    new_p = Path("data/raw/input-scenarios/202603251220_202512071232_202511040855_202511040855_complete-emissions.csv")

    base = load_timeseries_csv(
        base_p, index_columns=["model", "scenario", "variable", "region", "unit"], out_columns_type=int
    )
    new = load_timeseries_csv(
        new_p, index_columns=["model", "scenario", "variable", "region", "unit"], out_columns_type=int
    )

    for (model, scenario), msdf_base in base.groupby(["model", "scenario"]):
        msdf_new = new.openscm.mi_loc(pd.MultiIndex.from_tuples([(model, scenario)], names=["model", "scenario"]))

        overlapping_cols = np.intersect1d(msdf_base.columns, msdf_new.columns)
        msdf_base = msdf_base.loc[:, overlapping_cols]
        msdf_new = msdf_new.loc[:, overlapping_cols]

        diffs = compare_close(
            left=msdf_base,
            left_name=base_p.name,
            right=msdf_new,
            right_name=new_p.name,
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
