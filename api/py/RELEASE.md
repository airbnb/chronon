### Releasing

#### Requirements for releasing

Three main things to check:
  * Thrift api is up to date.
  * Tests are passing.
  * No straggling (unstaged/uncommited) files in repo.

The script `release.sh` takes care of these checks for convenience.

There are three python requirements to make a release:
 * tox: Module for testing. To run the tests run tox in the main project directory.
 * build: Module for building. To build run `python -m build` in the main project directory
 * twine: Module for publishing. To upload a distribution run `twine upload dist/<distribution>`

These can all be achieved in one install.
```
pip install -U tox build twine
```

Finally to publish, make sure you have your $HOME/.pypirc file configured.
```
[distutils]
  index-servers =
    local
    pypi
    chronon-pypi

[local]
  repository = # local artifactory
  username = # local username
  password = # token or password

[pypi]
  username = # username or __token__
  password = # password or token

# Or if using a project specific token
[chronon-pypi]
  repository = https://upload.pypi.org/legacy/
  username = __token__
  password = # Project specific pypi token.
```

#### Releasing

If the setup has been completed before. Running:

```
bash release.sh chronon-pypi
```
Should take care of all the checks and run the relevant commands.


#### Troubleshooting

Most common reason for failure is re-uploading a version that's already uploaded. Make sure the current version of
setup.py hasn't already been uploaded.
