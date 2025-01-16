# CHIP: Bazel Migration for Chronon

https://github.com/airbnb/chronon/issues/904

## Motivation

Migrating Chronon's build system to Bazel offers numerous benefits:
- **Hermetic Builds**: Ensures reproducibility by isolating builds from the environment.
- **Faster Builds**: Leverages caching and parallelism to speed up builds.
- **Reliable Tests**: Runs tests in isolated environments for consistent results.
- **Language Agnostic Builds**: Supports Scala, Java, Python, and other languages.
- **Extensibility**: Facilitates building for various Scala and Spark versions.

This migration will improve the developer experience and pave the way for better scalability and maintainability.

## Migration Phases

### **Phase 1: Initial Bazel Support**

#### Goals:
- Add Bazel build support for all components: `api`, `aggregator`, `service`, `online`, `flink`, and `spark`.
- Ensure builds work for Scala, Java, and Python.
- Enable Bazel to generate Thrift libraries from `api.thrift`.
- Preserve existing SBT builds to avoid disruptions.
- Avoid changes to CI during this phase.
- Ensure manual Bazel builds and tests are successful.

#### Example Build Command:
```bash
bazel build --config scala_2.12 --define spark_version=3.1 //aggregator
```

#### Deliverables:
- A Bazel build system that works alongside SBT.
- Documentation for building and testing with Bazel.
- Validation that builds and tests succeed manually.

PR: https://github.com/airbnb/chronon/pull/893
---

### **Phase 2: CI Integration with Bazel**

#### Goals:
- Update the CI pipeline to use Bazel as the primary build tool.
- Use Bazel for:
  - Building all components.
  - Running unit tests, linting, and coverage analysis.
- Remove SBT support as a build tool.

#### Deliverables:
- Fully functional CI pipeline using Bazel.
- Documentation on CI processes with Bazel.
- Deprecation and removal of SBT configuration.

---

### **Phase 3: Codebase Reorganization**

#### Goals:
- Refactor the codebase for improved organization and alignment with monorepo principles.
- Adopt a standardized folder structure.

#### Suggested Monorepo Folder Structure:
All sources for a particular component are colocated within the same folder, including source code, resources, 
test code, and build scripts. This structure ensures better organization and maintainability:
```
ai/chronon/
  api/
    *.thrift
    *.py
    *.scala
    test/
      *.scala
  aggregator/
    *.scala
    test/
      *.scala
  service/
    *.scala
    test/
      *.scala
  online/
    *.scala
    test/
      *.scala
  flink/
    *.scala
    test/
      *.scala
  spark/
    *.scala
    test/
      *.scala
  airflow/
    *.py
    test/
      *.py
```

#### Deliverables:
- Refactored codebase with the new folder structure.
- Updated documentation to reflect the reorganization.

---

## Summary
This CHIP outlines a phased approach to migrate Chronon’s build system to Bazel, ensuring minimal disruption while delivering substantial improvements in build reliability, speed, and maintainability. The migration will make the codebase future-proof and better aligned with monorepo principles, enabling seamless development and scaling across different components and versions.