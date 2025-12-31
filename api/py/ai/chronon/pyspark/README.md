# Chronon Python Interface for PySpark Environments

## Table of Contents
1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Core Components](#core-components)
4. [Flow of Execution](#flow-of-execution)
5. [Extending the Framework](#extending-the-framework)
6. [Environment Setup](#environment-setup)
7. [Current Limitations](#current-limitations)

## Introduction

The Chronon PySpark Interface provides a clean, object-oriented framework for executing Chronon feature definitions directly within a PySpark environment, like Databricks Notebooks. This interface streamlines the developer experience by removing the need to switch between multiple tools, allowing rapid prototyping and iteration of Chronon feature engineering workflows.

This library enables users to:
- Run and Analyze GroupBy and Join operations in a type-safe manner
- Execute feature computations within notebook environments like Databricks
- Implement platform-specific behavior while preserving a consistent interface
- Access JVM-based functionality directly from Python code

## Architecture Overview

### The Python-JVM Bridge

At the core of this implementation is the interaction between Python and the Java Virtual Machine (JVM):

```
Python Environment                  |                 JVM Environment
                                    |
 ┌─────────────────────┐            |            ┌─────────────────────┐
 │ Python Thrift Obj   │            |            │ Scala Thrift Obj    │
 │ (GroupBy, Join)     │─────┐      |      ┌────▶│ (GroupBy, Join)     │
 └─────────────────────┘     │      |      │     └─────────────────────┘
           │                 │      |      │               │
           ▼                 │      |      │               ▼
 ┌─────────────────────┐     │      |      │     ┌─────────────────────┐
 │ thrift_simple_json()│     │      |      │     │  PySparkUtils.parse │
 └─────────────────────┘     │      |      │     └─────────────────────┘
           │                 │      |      │               │
           ▼                 │      |      │               ▼
 ┌─────────────────────┐     │      |      │     ┌─────────────────────┐
 │ JSON String         │─────┼──────┼──────┼────▶│ Java Objects        │
 └─────────────────────┘     │      |      │     └─────────────────────┘
                             │      |      │               │
                             │      |      │               ▼
 ┌─────────────────────┐     │      |      │     ┌─────────────────────┐
 │ PySparkExecutable   │     │      |      │     │ PySparkUtils.run    │
 │ .run()              │─────┼──────┼──────┼────▶│ GroupBy/Join        │
 └─────────────────────┘     │      |      │     └─────────────────────┘
           ▲                 │      |      │               │
           │                 │      |      │               ▼
 ┌─────────────────────┐     │ Py4J Socket │     ┌─────────────────────┐
 │ Python DataFrame    │◀────┴──────┼──────┴─────│ JVM DataFrame       │
 └─────────────────────┘            |            └─────────────────────┘
                                    |                      
                                    |                     
                                    |            
                                    |            
                                    |            

```

- **Py4J**: Enables Python code to dynamically access Java objects, methods, and fields across the JVM boundary
- **PySpark**: Uses Py4J to communicate with the Spark JVM, translating Python calls into Spark's Java/Scala APIs
- **Thrift Objects**: Chronon features defined as Python thrift objects are converted to Java thrift objects for execution

This design ensures that Python users can access the full power of Chronon's JVM-based computation engine all from a centralized Python environment.

## Core Components

The framework is built around several core abstractions:

### PySparkExecutable

An abstract base class that provides common functionality for executing Chronon features via PySpark:

```python
class PySparkExecutable(Generic[T], ABC):
    """
    Abstract base class defining common functionality for executing features via PySpark.
    """
```

- Handles initialization with object and SparkSession
- Provides utilities for updating dates in sources and queries
- Manages the execution of underlying join sources

### Specialized Executables

Two specialized interfaces extend the base executable for different Chronon types:

- **GroupByExecutable**: Interface for executing GroupBy objects
- **JoinExecutable**: Interface for executing Join objects

These interfaces define type-specific behaviors for running and analyzing features.

### Platform Interface

A key abstraction that enables platform-specific behavior:

```python
class PlatformInterface(ABC):
    """
    Interface for platform-specific operations.
    """
```

This interface defines operations that vary by platform (Databricks, Jupyter, etc.) and must be implemented by platform-specific classes.

### Platform-Specific Implementations

Concrete implementations for specific notebook environments:

**Databricks:**
- **DatabricksPlatform**: Implements platform-specific operations for Databricks
- **DatabricksGroupBy**: Executes GroupBy objects in Databricks
- **DatabricksJoin**: Executes Join objects in Databricks

**Jupyter (JupyterLab, JupyterHub, classic notebooks):**
- **JupyterPlatform**: Implements platform-specific operations for Jupyter
- **JupyterGroupBy**: Executes GroupBy objects in Jupyter
- **JupyterJoin**: Executes Join objects in Jupyter

```
┌─────────────────────────┐
│    PySparkExecutable    │
│    (Generic[T], ABC)    │
├─────────────────────────┤
│ - obj: T                │
│ - spark: SparkSession   │
│ - jvm: JVMView          │
├─────────────────────────┤
│ + get_platform()        │
│ # _update_query_dates() │
│ # _update_source_dates()│
│ # print_with_timestamp()│
└───────────────┬─────────┘
                │
    ┌───────────┴────────────┐
    │                        │
┌───▼───────────────┐    ┌───▼───────────────┐
│  GroupByExecutable│    │  JoinExecutable   │
├───────────────────┤    ├───────────────────┤
│                   │    │                   │
├───────────────────┤    ├───────────────────┤
│ + run()           │    │ + run()           │
│ + analyze()       │    │ + analyze()       │
└────────┬──────────┘    └────────┬──────────┘
         │                        │
         │                        │
┌────────▼──────────┐    ┌────────▼──────────┐
│ DatabricksGroupBy │    │  DatabricksJoin   │
├───────────────────┤    ├───────────────────┤
│                   │    │                   │
├───────────────────┤    ├───────────────────┤
│ + get_platform()  │    │ + get_platform()  │
└───────────────────┘    └───────────────────┘

┌────────▼──────────┐    ┌────────▼──────────┐
│   JupyterGroupBy  │    │    JupyterJoin    │
├───────────────────┤    ├───────────────────┤
│                   │    │                   │
├───────────────────┤    ├───────────────────┤
│ + get_platform()  │    │ + get_platform()  │
└───────────────────┘    └───────────────────┘

┌─────────────────────────────┐
│   PlatformInterface         │
│         (ABC)               │
├─────────────────────────────┤
│ - spark: SparkSession       │
├─────────────────────────────┤
│ + register_udfs()           │
│ + get_executable_join_cls() │
│ + start_log_capture()       │
│ + end_log_capture()         │
│ + log_operation()           │
│ + drop_table_if_exists()    │
└───────────┬─────────────────┘
            │
    ┌───────┴───────┐
    │               │
┌───▼───────────────────┐  ┌───▼───────────────────┐
│  DatabricksPlatform   │  │   JupyterPlatform     │
├───────────────────────┤  ├───────────────────────┤
│ - dbutils: DBUtils    │  │ - log_file: str       │
├───────────────────────┤  ├───────────────────────┤
│ + register_udfs()     │  │ + register_udfs()     │
│ + get_executable_..() │  │ + get_executable_..() │
│ + start_log_capture() │  │ + start_log_capture() │
│ + end_log_capture()   │  │ + end_log_capture()   │
│ + get_databricks_user │  │ + get_jupyter_user()  │
└───────────────────────┘  └───────────────────────┘
```

## Flow of Execution

When a user calls a method like `DatabricksGroupBy(group_by, py_spark_session).run()`, the following sequence occurs:

1. **Object Preparation**:
    - The Python thrift object (GroupBy, Join) is copied and updated with appropriate dates (This interface is meant to be used to run prototypes over smaller date ranges and not full backfills)
    - Underlying join sources are executed if needed

2. **JVM Conversion**:
    - The Python thrift object is converted to JSON representation
    - The JSON is parsed into a Java thrift object on the JVM side via Py4J

3. **Execution**:
    - The JVM executes the computation using Spark
    - Results are captured in a Spark DataFrame on the JVM side

4. **Result Return**:
    - The JVM DataFrame is wrapped in a Python DataFrame object
    - The Python DataFrame is returned to the user

5. **Log Handling**:
    - Throughout the process, JVM logs are captured
    - Logs are printed in the notebook upon completion or errors

## Extending the Framework

### Implementing a New Platform Interface

To add support for a new notebook environment (e.g., Jupyter), follow these steps:

1. **Create a new platform implementation**:

```python
class JupyterPlatform(PlatformInterface):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        # Initialize Jupyter-specific components
    
    @override
    def register_udfs(self) -> None:
        # Register any necessary UDFs for Jupyter
        # Recall that UDFs are registered to the shared spark-sql engine
        # So you can register python and or scala udfs and use them on both spark sessions
        pass
    
    @override
    def get_executable_join_cls(self) -> type[JoinExecutable]:
        # Return the Jupyter-specific join executable class
        return JupyterJoin
        
    @override
    def start_log_capture(self, job_name: str) -> Any:
        # Start capturing logs in Jupyter
        pass
    
    @override
    def end_log_capture(self, capture_token: Any) -> None:
        # End log capturing and print the logs in Jupyter
        pass
```

2. **Create concrete executable implementations**:

```python
class JupyterGroupBy(GroupByExecutable):
    def __init__(self, group_by: GroupBy, spark_session: SparkSession):
        super().__init__(group_by, spark_session)
        # You can pass Jupyter specific parameters into to set metadata 
        # that allow you to customize things like:
        # - What namespace is written to 
        # - Table name prefixing (in the Databricks implementation we prefix the table name with the notebook username)
        # - Root dir for where your existing feature defs are if you want to import features that were defined in an IDE
        self.obj: GroupBy = self.platform.set_metadata(obj=self.obj)

    @override
    def get_platform(self) -> PlatformInterface:
        return JupyterPlatform(self.spark)

class JupyterJoin(JoinExecutable):
    def __init__(self, join: Join, spark_session: SparkSession):
        super().__init__(join, spark_session)
        # You can pass Jupyter specific parameters into to set metadata 
        # that allow you to customize things like:
        # - What namespace is written to 
        # - Table name prefixing (in the Databricks implementation we prefix the table name with the notebook username)
        # - Root dir for where your existing feature defs are if you want to import features that were defined in an IDE
        self.obj: Join = self.platform.set_metadata(obj=self.obj)

    @override
    def get_platform(self) -> PlatformInterface:
        return JupyterPlatform(self.spark)
```

### Key Methods to Override

When implementing a platform interface, pay special attention to these methods:

- **start_log_capture()** and **end_log_capture()**: Implement platform-specific log capturing

## Environment Setup

Whether you are running on Databricks, Jupyter, or any other PySpark environment, you need to include the Chronon JARs and their dependencies in your cluster's classpath. This is essential because the Python interface communicates with the JVM via Py4J to execute Chronon's Scala-based computation engine.

### Required JARs

The following Chronon modules must be available on the JVM classpath:

| Module | Description |
|--------|-------------|
| `chronon-api` | Thrift API definitions and data structures |
| `chronon-aggregator` | Core aggregation logic |
| `chronon-online` | Online serving utilities |
| `chronon-spark` | Spark integration and execution engine |

These JARs are published to Maven Central. You can find the latest versions at [Maven Central](https://search.maven.org/search?q=g:ai.chronon).

### Platform-Specific Setup

The exact method for adding JARs to your Spark cluster varies by platform:

- **Databricks**: Add JARs via cluster libraries (UI or cluster configuration)
- **Jupyter/Local Spark**: Use `spark.jars` configuration or `--jars` flag when starting Spark
- **EMR/Dataproc**: Include JARs in bootstrap actions or cluster configuration
- **Kubernetes**: Include JARs in your Spark driver/executor Docker images

Consult your platform's documentation for specific instructions on adding external JARs to the Spark classpath.

### Verifying Your Setup

Once the JARs are correctly configured, you can verify the setup by accessing the Chronon Scala code from Python:

```python
# This should work without errors if JARs are properly configured
spark_session._jsparkSession
jvm = spark_session._jvm
py_spark_utils = jvm.ai.chronon.spark.PySparkUtils

# Test parsing a simple GroupBy JSON (should not throw)
# py_spark_utils.parseGroupBy('{"metaData": {"name": "test"}}')
```

If you see a `Py4JJavaError` mentioning `ClassNotFoundException`, the Chronon JARs are not on the classpath.

### Python Dependencies

In addition to the JARs, you need the following Python packages:

- `pyspark` (tested on 3.1.x and 3.3.x)
- `thrift` (for Thrift object serialization)

### Log Configuration

For optimal debugging, configure your Spark cluster to write Chronon JVM logs to a single file. This allows the Python interface to capture and display relevant logs during execution. The specific log configuration is platform-dependent.

### Example Usage

#### Databricks Example

Here's a minimal example of using the Chronon Python interface in a Databricks notebook (assumes JARs are already configured):

```python
# Import the required modules
from pyspark.sql import SparkSession
from ai.chronon.pyspark.databricks import DatabricksGroupBy, DatabricksJoin
from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.group_by import Aggregation, Operation, Window, TimeUnit

# Define your GroupBy or Join object
my_group_by = GroupBy(...)

# Create an executable
executable = DatabricksGroupBy(my_group_by, spark)

# Run the executable
result_df = executable.run(start_date='20250101', end_date='20250107')
```

#### Jupyter Example

Here's a minimal example of using the Chronon Python interface in a Jupyter notebook:

```python
# Import the required modules
from pyspark.sql import SparkSession
from ai.chronon.pyspark.jupyter import JupyterGroupBy, JupyterJoin
from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.group_by import Aggregation, Operation, Window, TimeUnit

# Define your GroupBy or Join object
my_group_by = GroupBy(...)

# Create an executable (uses username prefix by default)
executable = JupyterGroupBy(my_group_by, spark)

# Run the executable
result_df = executable.run(start_date='20250101', end_date='20250107')

# You can also customize the name prefix and output namespace
executable_custom = JupyterGroupBy(
    my_group_by,
    spark,
    name_prefix="my_project",
    output_namespace="my_database",
    use_username_prefix=False
)
```

## Current Limitations

### Validation

The `validate()` method is not yet implemented in the open-source version. This method exists in some internal implementations to validate GroupBy and Join configurations before execution. Future contributions may add this functionality.

### StagingQuery

While `StagingQuery` is included in the type system for forward compatibility, a `StagingQueryExecutable` class is not yet implemented. Contributors are welcome to add this functionality.

---