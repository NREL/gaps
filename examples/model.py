# model.py
import numpy as np
from rex import Outputs


def run_model(lat, lon, a, b, c):
    """Example model that runs computation for a single site."""

    # simple computation for example purposes
    x = lat + lon
    return a * x**2 + b * x + c


def run(project_points, a, b, c, tag):
    """Run model on a single mode."""

    data = []
    for site in project_points:
        data.append(run_model(site.lat, site.lon, a, b, c))

    out_fp = f"results{tag}.h5"
    with Outputs(out_fp, "w") as fh:
        fh.meta = project_points.df
        fh.write_dataset("outputs", data=np.array(data), dtype="float32")

    return out_fp
