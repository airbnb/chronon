# generating Python code from thrift
# from https://git.corp.stripe.com/stripe-private-oss-forks/chronon/blob/master/devnotes.md#L13
export CHRONON_OS=/chronon
export CHRONON_API=$CHRONON_OS/api/py
alias materialize="PYTHONPATH=$CHRONON_API:$PYTHONPATH $CHRONON_API/ai/chronon/repo/compile.py"
thrift --gen py -out $CHRONON_OS/api/py/ai/chronon $CHRONON_OS/api/thrift/api.thrift

# install tox to run python tests
# from https://git.corp.stripe.com/stripe-private-oss-forks/chronon/blob/master/api/py/RELEASE.md#L13
pip install -U tox build twine