"""GAPs Project Points"""

import logging
from warnings import warn

import numpy as np
import pandas as pd

from rex.utilities import parse_table
from gaps.exceptions import (
    gapsIndexError,
    gapsKeyError,
    gapsRuntimeError,
)
from gaps.warn import gapsWarning
from gaps.utilities import project_points_from_container_or_slice


logger = logging.getLogger(__name__)


def _parse_sites(points):
    """Parse project points from list or slice

    Parameters
    ----------
    points : int | str | pd.DataFrame | slice | list
        Slice specifying project points, string pointing to a project
        points csv, or a DataFrame containing the effective csv
        contents. Can also be a single integer site value.

    Returns
    -------
    df : pd.DataFrame
        DataFrame of sites (gids) with corresponding args

    Raises
    ------
    gapsRuntimeError
        If points not flat.
    """
    try:
        points = project_points_from_container_or_slice(points)
    except TypeError as err:
        msg = (
            f"Cannot parse points data from {points}. If this input is a "
            "container, please ensure that the container is flat (no "
            "nested gid values)."
        )
        raise gapsRuntimeError(msg) from err

    return pd.DataFrame({"gid": points})


class ProjectPoints:
    """Class to manage site and SAM input configuration requests"""

    def __init__(self, points, **kwargs):
        """Initialize ProjectPoints

        Parameters
        ----------
        points : int | slice | list | tuple | str | pd.DataFrame | dict
            Slice specifying project points, string pointing to a
            project points csv, or a DataFrame containing the effective
            csv contents. Can also be a single integer site value.
        **kwargs
            Keyword-argument pairs to add to project points DataFrame.
            The key should be the column name, and the value should
            be the value to add under the column name. Values must
            either be a scalar or match the length of the DataFrame
            resulting from the `points` input.
        """
        self._df = self.split_range = None
        self._parse_points(points, **kwargs)

    def _parse_points(self, points, **kwargs):
        """Generate the project points df from inputs

        Parameters
        ----------
        points : int | str | pd.DataFrame | slice | list | dict
            Slice specifying project points, string pointing to a
            project points csv, or a DataFrame containing the effective
            csv contents. Can also be a single integer site value.

        Returns
        -------
        df : pd.DataFrame
            DataFrame of sites (gids) with corresponding args
        """
        try:
            self._df = parse_table(points)
        except ValueError:
            self._df = _parse_sites(points)

        if "gid" not in self._df.columns:
            msg = 'Project points data must contain "gid" column.'
            raise gapsKeyError(msg)

        for key, val in kwargs.items():
            self._df[key] = val

        self.split_range = (self._df.iloc[0].name, self._df.iloc[-1].name + 1)

        gids = self._df["gid"].to_numpy()
        if not np.array_equal(np.sort(gids), gids):
            msg = (
                "Points are not in sequential order and will be sorted! The "
                'original order is being preserved under column "points_order"'
            )
            warn(msg, gapsWarning)
            self._df["points_order"] = self._df.index.to_numpy()
            self._df = self._df.sort_values("gid")

        self._df = self._df.reset_index(drop=True)

    def __iter__(self):
        for __, row in self.df.iterrows():
            yield row

    def __getitem__(self, site_id):
        """Get the dictionary for the requested site

        Parameters
        ----------
        site_id : int | str
            Site number (gid) of interest (typically the resource gid).

        Returns
        -------
        site : pd.Series
            Pandas Series containing information for the site with the
            requested site_id.
        """

        if site_id not in self.df["gid"].to_numpy():
            msg = (
                f"Site {site_id} not found in this instance of "
                f"ProjectPoints. Available sites include: {self.gids}"
            )
            raise gapsKeyError(msg)

        return self.df.loc[self.df["gid"] == site_id].iloc[0]

    def __repr__(self):
        return (
            f"{self.__class__.__name__} with {len(self)} sites from gid "
            f"{self.gids[0]} through {self.gids[-1]}"
        )

    def __len__(self):
        """Length of this object is the number of sites"""
        return len(self.gids)

    @property
    def df(self):
        """pd.DataFrame: Project points DataFrame of site info"""
        return self._df

    @property
    def gids(self):
        """list: Gids (resource file index values) of sites"""
        return self.df["gid"].to_numpy().tolist()

    @property
    def sites_as_slice(self):
        """list | slice: Sites as slice or list if non-sequential"""
        # try_slice is what the sites list would be if it is sequential
        try_step = self.gids[1] - self.gids[0] if len(self.gids) > 1 else 1
        try_slice = slice(self.gids[0], self.gids[-1] + 1, try_step)
        try_list = list(range(*try_slice.indices(try_slice.stop)))

        if self.gids == try_list:
            return try_slice

        # cannot be converted to a sequential slice, return list
        return self.gids

    def index(self, gid):
        """Index location (iloc not loc) for a resource gid.

        Parameters
        ----------
        gid : int
            Resource GID found in the project points gid column.

        Returns
        -------
        ind : int
            Row index of gid in the project points DataFrame.
        """
        if gid not in self._df["gid"].to_numpy():
            msg = (
                f"Requested resource gid {gid} is not present in the project "
                f"points DataFrame. Cannot return row index."
            )
            raise gapsIndexError(msg)

        return np.where(self._df["gid"] == gid)[0][0]

    def join_df(self, df2, key="gid"):
        """Join df2 to the _df attribute using _df's gid as the join key

        This can be used to add site-specific data to the
        project_points, taking advantage of the `ProjectPoints`
        iterator/split functions such that only the relevant site data
        is passed to the analysis functions.

        Parameters
        ----------
        df2 : pd.DataFrame
            DataFrame to be joined to the :attr:`df` attribute (this
            instance of project points DataFrame). This likely contains
            site-specific inputs that are to be passed to parallel
            workers.
        key : str
            Primary key of df2 to be joined to the :attr:`df` attribute
            (this instance of the project points DataFrame). Primary key
            of the self._df attribute is fixed as the gid column.
        """
        # ensure df2 doesn't have any duplicate columns for suffix
        # reasons
        df2_cols = [c for c in df2.columns if c not in self._df or c == key]
        self._df = self._df.merge(
            df2[df2_cols],
            how="left",
            left_on="gid",
            right_on=key,
            copy=False,
            validate="1:1",
        )

    def get_sites_from_key(self, key, value):
        """Get a site list for which the key equals the value

        Parameters
        ----------
        key : str
            Name of key (column) in project points DataFrame.
        value : int | float | str | obj
            Value to look for under the ``key`` column.

        Returns
        -------
        sites : lis of ints
            List of sites (GID values) associated with the requested key
            and value. If the key or value does not exist, an empty list
            is returned.
        """
        if key not in self.df:
            return []

        return list(self.df.loc[self.df[key] == value, "gid"].values)

    def split(self, sites_per_split=100):
        """Split the project points into sub-groups by number of sites.

        Parameters
        ----------
        sites_per_split : int, optional
            Number of points in each sub-group. By default, `100`.

        Yields
        ------
        ProjectPoints
            A new ProjectPoints instance with up to `sites_per_split`
            number of points.
        """
        for ind in range(0, len(self.df), sites_per_split):
            yield self.__class__(self.df.iloc[ind : ind + sites_per_split])

    @classmethod
    def from_range(cls, split_range, points, **kwargs):
        """Create a ProjectPoints instance from a range indices.

        Parameters
        ----------
        split_range : 2-tuple
            Tuple containing the start and end index (iloc, not loc).
            Last index is not included.
        points : int | slice | list | tuple | str | pd.DataFrame | dict
            Slice specifying project points, string pointing to a
            project points csv, or a DataFrame containing the effective
            csv contents. Can also be a single integer site value.
        **kwargs
            Keyword-argument pairs to add to project points DataFrame.
            The key should be the column name, and the value should
            be the value to add under the column name. Values must
            either be a scalar or match the length of the DataFrame
            resulting from the `points` input.

        Returns
        -------
        ProjectPoints
            A new ProjectPoints instance with a range sampled from the
            points input according to ``split_range``.
        """
        pp = cls(points, **kwargs)
        start, end = split_range
        return cls(pp.df.iloc[start:end])
