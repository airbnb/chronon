import os
from setuptools import find_packages, setup

current_dir = os.path.dirname(__file__)
with open(os.path.join(current_dir, "README.md"), "r") as fh:
    long_description = fh.read()


with open(os.path.join(current_dir, "requirements/base.in"), "r") as infile:
    basic_requirements = [line for line in infile]


__version__ = "0.0.18"


setup(
    classifiers=[
        "Programming Language :: Python :: 3.7"
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    scripts=['ai/chronon/repo/explore.py', 'ai/chronon/repo/compile.py', 'ai/chronon/repo/run.py'],
    description="Chronon python API library",
    include_package_data=True,
    install_requires=basic_requirements,
    name="chronon-ai",
    packages=find_packages(),
    extras_require={
        # Extra requirement to have access to cli commands in python2 environments.
        "pip2compat": ["click<8"]
    },
    python_requires=">=3.7",
    url=None,
    version=__version__,
    zip_safe=False,
)
