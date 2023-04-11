# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""Custom Warning for GAPs. """
import logging

logger = logging.getLogger("gaps")


class gapsWarning(UserWarning):
    """Generic gaps Warning."""

    def __init__(self, *args, **kwargs):
        """Init exception and broadcast message to logger."""
        super().__init__(*args, **kwargs)
        if args:
            logger.warning(str(args[0]), stacklevel=2)


class gapsCollectionWarning(gapsWarning):
    """gaps Collection waring."""


class gapsHPCWarning(gapsWarning):
    """gaps HPC warning."""


class gapsDeprecationWarning(gapsWarning, DeprecationWarning):
    """gaps deprecation warning."""
