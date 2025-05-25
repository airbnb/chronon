# Chronon Python Interface for PySpark Environments

## Table of Contents
1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Core Components](#core-components)
4. [Flow of Execution](#flow-of-execution)
5. [Extending the Framework](#extending-the-framework)
6. [Setup and Dependencies](#setup-and-dependencies)

## Introduction

The Chronon PySpark Interface provides a clean, object-oriented framework for executing Chronon feature definitions directly within a PySpark environment, like Notebooks Notebooks. This interface streamlines the developer experience by removing the need to switch between multiple tools, allowing rapid prototyping and iteration of Chronon feature engineering workflows.

This library enables users to:
- Run and Analyze GroupBy and Join operations in a type-safe manner
- Execute feature computations within notebook environments like Notebooks
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

This interface defines operations that vary by platform (Notebooks, Jupyter, etc.) and must be implemented by platform-specific classes.

### Platform-Specific Implementations

Concrete implementations for specific notebook environments:

- **NotebooksPlatform**: Implements platform-specific operations for Notebooks
- **NotebooksGroupBy**: Executes GroupBy objects in Notebooks
- **NotebooksJoin**: Executes Join objects in Notebooks

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
│ NotebooksGroupBy │    │  NotebooksJoin   │
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
            │
┌───────────▼────────────────┐
│  NotebooksPlatform        │
├────────────────────────────┤
│ - dbutils: DBUtils         │
├────────────────────────────┤
│ + register_udfs()          │
│ + get_executable_join_cls()│
│ + start_log_capture()      │
│ + end_log_capture()        │
│ + get_notebooks_user()    │
└────────────────────────────┘
```

## Flow of Execution

When a user calls a method like `NotebooksGroupBy(group_by, py_spark_session).run()`, the following sequence occurs:

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
        # - Table name prefixing (in the Notebooks implementation we prefix the table name with the notebook username)
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
        # - Table name prefixing (in the Notebooks implementation we prefix the table name with the notebook username)
        # - Root dir for where your existing feature defs are if you want to import features that were defined in an IDE
        self.obj: Join = self.platform.set_metadata(obj=self.obj)

    @override
    def get_platform(self) -> PlatformInterface:
        return JupyterPlatform(self.spark)
```

### Key Methods to Override

When implementing a platform interface, pay special attention to these methods:

- **start_log_capture()** and **end_log_capture()**: Implement platform-specific log capturing

## Setup and Dependencies

### Requirements

1. **Spark Dependencies**: The Chronon Java/Scala JARs must be included in your Spark cluster

2. **Python Dependencies**:
    - pyspark (tested on both 3.1 and 3.3)

3. **Log File**: You will need to make sure that your Chronon JVM logs are writting to single file. This is generally platform specific.

### Example Setup

Here's a minimal example of setting up and using the Chronon Python interface in a Notebooks notebook. It assumes that you have already included the necessary jars in your cluster dependencies.

```python
# Import the required modules
from pyspark.sql import SparkSession
from ai.chronon.pyspark.notebooks import NotebooksGroupBy, NotebooksJoin
from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.group_by import Aggregation, Operation, Window, TimeUnit

# Define your GroupBy or Join object
my_group_by = GroupBy(...)

# Create an executable
executable = NotebooksGroupBy(my_group_by, spark)

# Run the executable
result_df = executable.run(start_date='20250101', end_date='20250107')
```

---