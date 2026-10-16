#!/bin/bash
# Runs the spark/aggregator test classes assigned to this CircleCI container.
#
# Usage: run_split_tests.sh <scala-version>   e.g. run_split_tests.sh 2.13.6
#
# - Test classes are discovered from *Test.scala files and converted to fully-qualified class
#   names, so `circleci tests split --timings-type=classname` can match them against the
#   <testcase classname="..."> attribute in the stored JUnit reports. (The previous
#   `--timings-type=filename` never matched anything, because sbt's JUnit XML carries no file
#   attribute, so the split silently fell back to splitting by name and was badly unbalanced.)
# - All classes assigned to this container run in a single sbt invocation instead of one sbt
#   process per class, which removes the repeated sbt startup and incremental compile checks.
set -euo pipefail

SCALA_VERSION="${1:?scala version required, e.g. 2.12.12}"

# Fetcher tests run in their own job (see config.yml).
TEST_FILES=$(find spark/src/test/scala aggregator/src/test/scala -name "*Test.scala" | grep -v "FetcherTest" | sort)

# file path -> "module fully.qualified.ClassName". The class name is read from the file rather
# than derived from the file name so files like JoinBasicTest.scala (class JoinBasicTests) are run.
all_classes() {
  for filepath in $TEST_FILES; do
    case "$filepath" in
      spark/*)      module="spark_uber"; src_root="spark/src/test/scala/" ;;
      aggregator/*) module="aggregator"; src_root="aggregator/src/test/scala/" ;;
      *) continue ;;
    esac
    package=$(dirname "${filepath#"$src_root"}" | sed 's|/|.|g')
    for class in $(grep -ohE '^class [A-Za-z0-9_]+Tests?\b' "$filepath" | awk '{print $2}'); do
      echo "$module $package.$class"
    done
  done
}

ALL=$(all_classes)
# Split on the class name only; map back to the module afterwards.
MINE=$(echo "$ALL" | awk '{print $2}' | sort | circleci tests split --split-by=timings --timings-type=classname)

SPARK_CLASSES=""
AGG_CLASSES=""
for class in $MINE; do
  module=$(echo "$ALL" | awk -v c="$class" '$2 == c {print $1; exit}')
  case "$module" in
    spark_uber) SPARK_CLASSES="$SPARK_CLASSES $class" ;;
    aggregator) AGG_CLASSES="$AGG_CLASSES $class" ;;
  esac
done

echo "Scala $SCALA_VERSION / spark_uber classes:$SPARK_CLASSES"
echo "Scala $SCALA_VERSION / aggregator classes:$AGG_CLASSES"

SBT_COMMANDS="++ $SCALA_VERSION"
if [[ -n "$SPARK_CLASSES" ]]; then
  SBT_COMMANDS="$SBT_COMMANDS; spark_uber/testOnly$SPARK_CLASSES"
fi
if [[ -n "$AGG_CLASSES" ]]; then
  SBT_COMMANDS="$SBT_COMMANDS; aggregator/testOnly$AGG_CLASSES"
fi

sbt "$SBT_COMMANDS"
