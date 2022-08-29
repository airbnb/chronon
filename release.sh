set -euxo pipefail
echo "pushing to maven with version $CHRONON_VERSION"
# GPG_TTY=$(tty) sbt +publishSigned
echo "pushing to pypi with version $CHRONON_VERSION"
thrift --gen py -out api/py/ai/chronon api/thrift/api.thrift
pushd api/py || exit
python3 -m build
# twine upload dist/*
rm -rf dist/*
popd || exit
echo "check "