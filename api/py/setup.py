import os
import sys

from setuptools import find_packages, setup
from setuptools.command.install import install

with open("README.md", "r") as fh:
    long_description = fh.read()


with open("requirements/base.in", "r") as infile:
    basic_requirements = [line for line in infile]


__version__ = "0.0.1"


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches package version.
    git tag looks like zl-py-0.0.1
    """
    description = 'verify that the git tag matches package version'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')
        if not tag:
            sys.exit("Git tag does not exist.")
        tag_version = tag.split('-')[-1]
        if tag_version != __version__:
            info = "Git tag version: {0} does not match the version of this app: {1}".format(
                tag_version, __version__
            )
            sys.exit(info)


setup(
    classifiers=[
        "Programming Language :: Python :: 3.7"
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    scripts=['ai/zipline/repo/compile.py'],
    description="Zipline python API library",
    include_package_data=True,
    install_requires=basic_requirements,
    name="zipline-ai-dev",
    packages=find_packages(),
    python_requires=">=3.7",
    url=None,
    version=__version__,
    zip_safe=False,
    cmdclass={
        'verify': VerifyVersionCommand,
    }
)
