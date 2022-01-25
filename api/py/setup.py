from setuptools import find_packages, setup


with open("README.md", "r") as fh:
    long_description = fh.read()


with open("requirements/base.in", "r") as infile:
    basic_requirements = [line for line in infile]


__version__ = "0.0.23"


setup(
    classifiers=[
        "Programming Language :: Python :: 3.7"
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    scripts=['ai/zipline/repo/explore.py', 'ai/zipline/repo/compile.py', 'ai/zipline/repo/run.py'],
    description="Zipline python API library",
    include_package_data=True,
    install_requires=basic_requirements,
    name="zipline-ai-dev",
    packages=find_packages(),
    python_requires=">=3.7",
    url=None,
    version=__version__,
    zip_safe=False,
)
