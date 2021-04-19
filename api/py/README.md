### Zipline Python API


#### Overview

Zipline Python API for materializing configs to be run by the Zipline Engine.


#### Set up for publishing.

Create your `~/.pypirc` file with your credentials for pypi repository.

```
[distutils]
index-servers =
  pypi
  pypitest
  local

[pypi]
username: nalgeon  # replace with your PyPI username

[pypitest]
repository: https://test.pypi.org/legacy/
username: nalgeon  # replace with your TestPyPI username

[local]
repository: <local artifactory repository>
```

Generate the required thrift modules, update the version and run the respective command to publish to the desired
repository:

```
python setup.py sdist upload -r { pypi | pypitest | local }
```
