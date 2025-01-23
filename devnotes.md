# Development

## Intro

### Commands

***All commands assume you are in the root directory of this project***. For me, that looks like `~/repos/chronon`.

---

## Prerequisites

### Environment Setup

Add the following to your shell run command files, e.g., `~/.bashrc` (primarily for SBT users):

```shell
export CHRONON_OS=<path/to/chronon/repo>
export CHRONON_API=$CHRONON_OS/api/py
alias materialize="PYTHONPATH=$CHRONON_API:$PYTHONPATH $CHRONON_API/ai/chronon/repo/compile.py"
```

### Install Thrift (Version 0.13)

This step is relevant to both Bazel and SBT users. Thrift is a dependency for compile. The latest version, 0.14, is incompatible with Hive Metastore. Install version 0.13:

Thrift is a dependency for compile. The latest version, 0.14, is incompatible with Hive Metastore. Install version 0.13:

```shell
brew tap cartman-kai/thrift
brew install thrift@0.13
```

### Install Python Dependencies for the API

```shell
python3 -m pip install -U tox build
```

---

## Build Systems

This project supports both Bazel and SBT. Bazel's hermetic nature simplifies setup compared to SBT, as it doesn't require extensive environment configuration. Choose the system that works best for your workflow.

### Bazel Setup

1. Bazel is hermetic and does not require additional environment setup, except for the installation of Thrift@0.13.

2. Ensure you have a `WORKSPACE` file and `BUILD` files in the appropriate directories.

3. Common Bazel Commands:
    - Build all targets:
      ```shell
      bazel build //...
      ```
    - Build Specific targets:
      ```shell
      bazel build //module:target
      bazel build //aggregator:aggregator
      bazel build //api:api-lib
      bazel build //online:online
      bazel build //service:service
      bazel build //spark:spark
      ```
    - Run tests:
      ```shell
      bazel test //...
      ```
    - Run specific tests:
      ```shell
      bazel test //module:TestName
      bazel test //aggregator:test
      bazel test //api:test
      ```
    - Build a fat jar:
      ```shell
      bazel build //module:deploy.jar
      bazel build //spark:spark-assembly_deploy.jar
      ```

### SBT Setup

1. Open the project in IntelliJ from the `build.sbt` file (at the root level).

2. Configure IntelliJ:

    - Mark these directories as `Sources Root`:
        - `aggregator/src/main/scala`
        - `api/src/main/scala`
        - `spark/src/main/scala`
    - Mark these directories as `Test Root`:
        - `aggregator/src/test/scala`
        - `api/src/test/scala`
        - `spark/src/test/scala`

3. Common SBT Commands:

    - Build all artifacts:
      ```shell
      sbt package
      ```
    - Build a fat jar:
      ```shell
      sbt assembly
      ```
    - Run all tests:
      ```shell
      sbt test
      ```
    - Run specific tests:
      ```shell
      sbt "testOnly *<Module>"
      ```

### Generate Python Thrift Definitions

#### With SBT:

```shell
sbt py_thrift
```

#### With Bazel:

```shell
bazel build //api/thrift:api-models-py
```

## Dependency Management

### Bazel:

#### Adding Java/Scala Dependencies

1. Update Maven Dependencies:
   - Locate `jvm/<repo>_repo.bzl` file (e.g., `jvm/maven_repo.bzl`)
   - Add/update the dependency declaration:
   ```python
   artifacts = [
    "org.apache.thrift:libthrift:0.13.0",
   ]
   ```

2. Reference in BUILD files:
   - Add the dependency to your target's deps attribute:
   ```python
   scala_library(
       name = "my_library",
       srcs = glob(["src/main/scala/**/*.scala"]),
       deps = [
           "@maven//:org_example_library",  # Note: colons become underscores
           # Other dependencies...
       ],
   )
   ```

3. Refresh Bazel's dependency cache:
   ```shell
   bazel clean --expunge
   bazel build //...
   ```

#### Adding Python Dependencies

1. Update `requirements.txt`:
   - Add your dependency with version:
   ```text
   requests==2.28.1
   numpy>=1.21.0
   ```

2. Update pip dependencies:
   ```shell
   bazel run //:pip.update
   ```

3. Reference in BUILD files:
   ```python
   py_library(
       name = "my_python_lib",
       srcs = glob(["*.py"]),
       deps = [
           requirement("requests"),
           requirement("numpy"),
       ],
   )
   ```

Graph view of dependencies:

```shell
bazel query 'deps(//module:target)' --output graph
```
---

## Materializing Configurations

```shell
materialize --input_path=<path/to/conf>
```

---

## Testing

### Bazel:

Run all tests:

```shell
bazel test //...
```

Run a specific test module:

```shell
bazel test //module:SpecificTest
```

### SBT:

Run all tests:

```shell
sbt test
```

Run specific tests:

```shell
sbt "testOnly *<Module>"
```

---

## Dependency Management

### Bazel:

Graph view of dependencies:

Bazel's dependency graph is hermetic and reproducible. It ensures that all dependencies are defined explicitly, avoiding issues caused by system-level or implicit dependencies. This contrasts with SBT, which relies on configuration files and environment settings to resolve dependencies.

```shell
bazel query 'deps(//module:target)' --output graph
```

### SBT:

Graph-based view of dependencies:

```shell
sbt dependencyBrowseGraph
```

Tree-based view of dependencies:

```shell
sbt dependencyBrowseTree
```

---

## Artifact Building

### Bazel:

Default settings in .bazelrc

Build all artifacts:

```shell
bazel build //...
```

Build a specific artifact:

```shell
bazel build //module:artifact_name
```

Build a scala version specific artifact:

```shell
bazel build --config scala_2.12 //module:artifact_name
```

Build a spark version specific artifact:

```shell
bazel build --config spark_3.5 //module:artifact_name
```

Build deploy jar aka Uber jar or fat jar:

```shell
bazel build --config scala_2.13 --config spark_3.5 //spark:spark-assembly_deploy.jar
```


### SBT:

Build all artifacts:

```shell
sbt package
```

Build Python API:

```shell
sbt python_api
```

Build a fat jar:

```shell
sbt assembly
```

---

## Publishing Artifacts

### Using SBT

Publish all artifacts:

```shell
sbt publish
```

### Using Bazel

Publish to a custom repository:

```shell
bazel run //module:publish
```

---

## Documentation

Generate documentation via Sphinx:

```shell
sbt sphinx
```

---

## Additional Notes

For Bazel-specific troubleshooting, refer to the Bazel documentation: [https://bazel.build](https://bazel.build)

For SBT-specific troubleshooting, refer to the SBT documentation: [https://www.scala-sbt.org](https://www.scala-sbt.org)
