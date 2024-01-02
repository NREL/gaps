"""
setup.py
"""
from pathlib import Path

from setuptools import setup, find_packages


HERE = Path(__file__).parent.resolve()

with open(HERE / "README.rst", encoding="utf-8") as f:
    README = f.read()

with open(HERE / "gaps" / "version.py", encoding="utf-8") as f:
    VERSION = f.read().split("=")[-1].strip().strip('"').strip("'")

with open("requirements.txt") as f:
    INSTALL_REQUIREMENTS = f.read().splitlines()


DEV_REQUIREMENTS = ["black", "pylint", "jupyter", "pipreqs"]
TEST_REQUIREMENTS = ["pytest", "pytest-cov", "h5py"]
DOC_REQUIREMENTS = ["make", "ghp-import", "numpydoc", "pandoc"]
DESCRIPTION = (
    "National Renewable Energy Laboratory's (NREL's) Geospatial Analysis "
    "Pipelines (GAPs) framework"
)


setup(
    name="NREL-gaps",
    version=VERSION,
    description=DESCRIPTION,
    long_description=README,
    author="Paul Pinchuk",
    maintainer_email="ppinchuk@nrel.gov",
    url="https://nrel.github.io/gaps/",
    packages=find_packages(),
    package_dir={"gaps": "gaps"},
    license="BSD 3-Clause",
    zip_safe=False,
    keywords="gaps",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
    test_suite="tests",
    install_requires=INSTALL_REQUIREMENTS,
    extras_require={
        "test": TEST_REQUIREMENTS,
        "dev": TEST_REQUIREMENTS + DEV_REQUIREMENTS,
        "docs": TEST_REQUIREMENTS + DEV_REQUIREMENTS + DOC_REQUIREMENTS,
    },
    entry_points={"console_scripts": ["gaps=gaps._cli:main"]},
)
