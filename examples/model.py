# model.py
from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np
import pandas as pd
from rex import Outputs


def run_model(lat, lon, a, b, c):
    """Example model that runs computation for a single site."""

    # simple computation for example purposes
    x = lat + lon
    return a * x**2 + b * x + c


def run(project_points, a, b, c, tag):
    """Run model on a single node."""

    data = []
    for site in project_points:
        data.append(run_model(site.lat, site.lon, a, b, c))

    out_fp = f"results{tag}.h5"
    with Outputs(out_fp, "w") as fh:
        fh.meta = project_points.df
        fh.write_dataset("outputs", data=np.array(data), dtype="float32")

    return out_fp


def run_parallelized(project_points, a, b, c, tag, max_workers=None):
    """Run model on a single node with multiprocessing."""

    out_fp = f"results{tag}.h5"
    Outputs.init_h5(
        out_fp,
        ["outputs"],
        shapes={"outputs": (project_points.df.shape[0],)},
        attrs={"outputs": None},
        chunks={"outputs": None},
        dtypes={"outputs": "float32"},
        meta=project_points.df,
    )

    futures = {}
    with ProcessPoolExecutor(max_workers=max_workers) as exe:
        for site in project_points:
            future = exe.submit(run_model, site.lat, site.lon, a, b, c)
            futures[future] = site.gid

    with Outputs(out_fp, "a") as out:
        for future in as_completed(futures):
            gid = futures.pop(future)
            ind = project_points.index(gid)
            out["outputs", ind] = future.result()

    return out_fp


def model_preprocessor(config, param_csv_fp):
    """Preprocess user input."""
    df = pd.read_csv(param_csv_fp)
    config["a"] = list(df["a"])
    config["b"] = list(df["b"])

    return config


def another_model(x, y, z):
    """Execute another model"""
    ...
    return f"out_file{x}_{y}_{z}.csv"


class MyFinalModel:
    """Execute the last model"""

    def __init__(self, m, n):
        self.m = m
        self.n = n
        ...

    def execute(self, o, p):
        """Execute the model"""
        ...
        return f"out_path{self.m}_{self.n}_{o}_{p}.out"
