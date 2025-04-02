"""Custom Warning for GAPs"""

import logging

logger = logging.getLogger("gaps")


class gapsWarning(UserWarning):  # noqa: N801
    """Generic gaps Warning"""

    def __init__(self, *args, **kwargs):
        """Init exception and broadcast message to logger"""
        super().__init__(*args, **kwargs)
        if args:
            logger.warning(str(args[0]), stacklevel=2)


class gapsCollectionWarning(gapsWarning):  # noqa: N801
    """gaps Collection waring"""


class gapsHPCWarning(gapsWarning):  # noqa: N801
    """gaps HPC warning"""


class gapsDeprecationWarning(gapsWarning, DeprecationWarning):  # noqa: N801
    """gaps deprecation warning"""
