# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""Custom Warning for GAPs. """
import sys
import logging

logger = logging.getLogger("gaps")


class gapsWarning(UserWarning):
    """Generic gaps Warning."""

    def __init__(self, *args, **kwargs):
        """Init exception and broadcast message to logger."""
        super().__init__(*args, **kwargs)
        if args:
            l_kwargs = {}
            if sys.version_info[1] >= 8:  # pragma: no cover
                l_kwargs["stacklevel"] = 2
            logger.warning(str(args[0]), **l_kwargs)


class gapsCollectionWarning(gapsWarning):
    """gaps Collection waring."""


class gapsHPCWarning(gapsWarning):
    """gaps HPC warning."""
