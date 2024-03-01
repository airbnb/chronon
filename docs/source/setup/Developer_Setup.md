# Developer Setup

The Chronon developer experience consists of the following steps:

1. Authoring Chronon files in python within a specific directory.
2. Compiling the files using the `compile.py` CLI tool
3. Running those files with the `run.py` CLI tool
4. Inspecting the results and iterating
5. Merging the file to productionize it

## The main Chronon directory

This is the directory within a version controlled repository where users will author, code-review and merge their Chronon files.

If you're using Airflow, the easiest thing to do is to put this directory directly within your Airflow repository, so that the orchestration DAGs have easy access to the Chronon files that they need to run jobs.

This directory must be structured as:

```
chronon/
├─ group_bys/
│  ├─ team_1/
│  │  ├─ some_features.py
├─ joins/
│  ├─ team_1/
│  │  ├─ some_join.py
├─ teams.json
├─ scripts/
│  ├─ spark_submit.sh
```

Key points:
1. There is a `teams.json` file within the root directory, that contains configuration for various teams that are allowed to run Chronon jobs.
2. There are `group_bys` and `joins` subdirectories inside the root directory, under which there are team directories. Note that the team directory names must match what is within `teams.json`
3. Within each of these team directories are the actual user-written chronon files. Note that there can be sub-directories within each team directory for organization if desired.

For an example setup of this directory, see the [Sample](https://github.com/airbnb/chronon/tree/main/api/py/test/sample) that is also mounted to the docker image that is used in the Quickstart guide.

You can also use the following command to create a scratch directory from your `cwd`:

```sh
curl -s https://chronon.ai/init.sh | $SHELL
```

## The Chronon python library

In addition to a provisioned Chronon directory, developers need to have the Chronon python library installed in their developer environment. This can be easily done via pip with:

```sh
pip install chronon-ai
```

The main directory and the python library is all that is required to run the developer steps mentioned at the top of this document.

Note that depending on your preferred developer experience, users may be authoring Chronon objects locally then syncing them to a remote machine for execution on the spark cluster. There a number of ways to enable this, the simplest being a `sync` command that copies over files from local to remote. However, there could be better flows from a UX perspective, depending on your existing infrastructure for running jobs on the cluster. Feel free to reach out in the [community Discord channel](https://discord.gg/GbmGATNqqP) if you wish to discuss ways to implement a better developer experience.
