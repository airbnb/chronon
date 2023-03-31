import os
import re
from setuptools import find_packages, setup

current_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(current_dir, "README.md"), "r") as fh:
    long_description = fh.read()


with open(os.path.join(current_dir, "requirements/base.in"), "r") as infile:
    basic_requirements = [line for line in infile]


__version__ = "local"
__branch__ = "master"
def get_version():
    version_str = os.environ.get("CHRONON_VERSION_STR", __version__)
    branch_str = os.environ.get("CHRONON_BRANCH_STR", __branch__)
    # Replace "-SNAPSHOT" with ".dev"
    version_str = version_str.replace("-SNAPSHOT", ".dev")
    # If the prefix is the branch name, then convert it as suffix after '+' to make it Python PEP440 complaint
    if version_str.startswith(branch_str + "-"):
        version_str = "{}+{}".format(
            version_str.replace(branch_str + "-", ""),
            branch_str
        )

    # Replace multiple continuous '-' or '_' with a single period '.'.
    # In python version string, the label identifier that comes after '+', is all separated by periods '.'
    version_str = re.sub(r'[-_]+', '.', version_str)

    return version_str

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
    version=get_version(),
    zip_safe=False
)
