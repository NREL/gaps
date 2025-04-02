"""GAPs Documentation"""

import logging
from pathlib import Path

from .collection import Collector
from .pipeline import Pipeline
from .project_points import ProjectPoints
from .status import Status
from ._version import __version__


GAPS_DIR = Path(__file__).parent
REPO_NAME = __name__
TEST_DATA_DIR = GAPS_DIR.parent / "tests" / "data"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.setLevel("DEBUG")
logger.propagate = False
