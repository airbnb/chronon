#!/usr/bin/env bash

set -euxo pipefail

LOCAL_VERSION="${VARIABLE:-local}"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "Using local version $LOCAL_VERSION.."

sbt clean package assembly

mvn_local_deploy() {
    local ARTIFACT_ID=$1
    local ARTIFACT_PATH=$2
    mvn  install:install-file  \
        -Dfile="$SCRIPT_DIR"/"$ARTIFACT_PATH" \
        -Dversion=$LOCAL_VERSION \
        -DgroupId=ai.zipline \
        -DartifactId="$ARTIFACT_ID" \
        -Dpackaging=jar
}

mvn_local_deploy spark_uber_2.11 spark/target/scala-2.11/spark-assembly-0.1.0-SNAPSHOT.jar
mvn_local_deploy online_2.11 online/target/scala-2.11/online_2.11-0.1.0-SNAPSHOT.jar
mvn_local_deploy api_2.11 api/target/scala-2.11/api_2.11-0.1.0-SNAPSHOT.jar
mvn_local_deploy aggregator_2.11 aggregator/target/scala-2.11/aggregator_2.11-0.1.0-SNAPSHOT.jar
