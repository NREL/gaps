"""GAPs utilities"""

import sys
import copy
import logging
import collections
import contextlib
from enum import Enum
from pathlib import Path

from gaps.exceptions import gapsValueError

logger = logging.getLogger(__name__)


class CaseInsensitiveEnum(str, Enum):
    """A string enum that is case insensitive"""

    def __new__(cls, value):
        """Create new enum member"""

        value = value.lower().strip()
        obj = str.__new__(cls, value)
        obj._value_ = value
        return cls._new_post_hook(obj, value)

    def __format__(self, format_spec):
        return str.__format__(self._value_, format_spec)

    @classmethod
    def _missing_(cls, value):
        """Convert value to lowercase before lookup"""
        if value is None:
            return None

        value = value.lower().strip()
        for member in cls:
            if member.value == value:
                return member

        return None

    @classmethod
    def _new_post_hook(cls, obj, value):  # noqa: ARG003
        """Hook for post-processing after __new__"""
        return obj

    @classmethod
    def members_as_str(cls):
        """Set of enum members as strings"""
        return {member.value for member in cls}


def project_points_from_container_or_slice(project_points):
    """Parse project point input into a list of GIDs

    Parameters
    ----------
    project_points : numeric | container
        A number or container of numbers that holds GID values. If a
        mapping (e.g. dict, pd.DataFrame, etc), a "gid" must map to the
        desired values.

    Returns
    -------
    list
        A list of integer GID values.
    """
    with contextlib.suppress((KeyError, TypeError, IndexError)):
        project_points = project_points["gid"]

    with contextlib.suppress(AttributeError):
        project_points = project_points.to_numpy()

    with contextlib.suppress(AttributeError):
        project_points = _slice_to_list(project_points)

    try:
        return [int(g) for g in project_points]
    except TypeError:
        return [int(g) for g in [project_points]]


def _slice_to_list(inputs_slice):
    """Convert a slice to a list of values"""
    start = inputs_slice.start or 0
    end = inputs_slice.stop
    if end is None:
        msg = "slice must be bounded!"
        raise gapsValueError(msg)
    step = inputs_slice.step or 1
    return list(range(start, end, step))


def recursively_update_dict(existing, new):
    """Update a dictionary recursively

    Parameters
    ----------
    existing : dict
        Existing dictionary to update. Dictionary is copied before
        recursive update is applied.
    new : dict
        New dictionary with data to add to `existing`.

    Returns
    -------
    dict
        Existing dictionary with data updated from new dictionary.
    """

    existing = copy.deepcopy(existing)

    for key, val in new.items():
        if isinstance(val, collections.abc.Mapping):
            existing[key] = recursively_update_dict(existing.get(key, {}), val)
        else:
            existing[key] = val
    return existing


def resolve_path(path, base_dir):
    """Resolve a file path represented by the input string.

    This function resolves the input string if it resembles a path.
    Specifically, the string will be resolved if it starts  with
    "``./``" or "``..``", or it if it contains either "``./``" or
    "``..``" somewhere in the string body. Otherwise, the string
    is returned unchanged, so this function *is* safe to call on any
    string, even ones that do not resemble a path.
    This method delegates the "resolving" logic to
    :meth:`pathlib.Path.resolve`. This means the path is made
    absolute, symlinks are resolved, and "``..``" components are
    eliminated. If the ``path`` input starts with "``./``" or
    "``..``", it is assumed to be w.r.t the config directory, *not*
    the run directory.

    Parameters
    ----------
    path : str
        Input file path.
    base_dir : path-like
        Base path to directory from which to resolve path string
        (typically current directory).

    Returns
    -------
    str
        The resolved path.
    """
    base_dir = Path(base_dir)

    if path.startswith("./"):
        path = base_dir / Path(path[2:])
    elif path.startswith(".."):
        path = base_dir / Path(path)
    elif "./" in path:  # this covers both './' and '../'
        path = Path(path)

    with contextlib.suppress(AttributeError):  # `path` is still a `str`
        path = path.expanduser().resolve().as_posix()

    return path


def _is_sphinx_build():
    """``True`` if sphinx is in system modules, else ``False``"""
    return "sphinx" in sys.modules
