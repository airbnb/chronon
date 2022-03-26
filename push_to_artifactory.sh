#!/usr/bin/env bash

set -euxo pipefail

MINOR_VERSION=$1
# TODO drop airbnb URL - migrate to maven central before open sourcing.
REPO_URL=https://artifactory.d.musta.ch/artifactory/maven-airbnb-releases
REPO_ID=airbnb

GIT_BRANCH=$(git branch --show-current)

if [[ "$GIT_BRANCH" != "master" && "$MINOR_VERSION" != *[tT]est* ]]
then
  echo "Your branch($GIT_BRANCH) is not master, but the version doesn't contain 'test'"
  exit
fi

if [[ "$GIT_BRANCH" == "master" && "$MINOR_VERSION" == [0-9]+ ]]
then
  echo "Updating minor version on branch master with production version. So doing a git pull"
  git pull
fi

GIT_SHA=$(git log --pretty=format:'%h' -n 1)
NEW_TAG=release-0.0.$MINOR_VERSION
echo "Tagging $GIT_BRANCH@$GIT_SHA with $NEW_TAG (minor version incremented)"
git tag -a -m '' $NEW_TAG
git push origin $NEW_TAG
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "Building jars"
sbt +clean +package +assembly spark_embedded/assembly

mvn_deploy() {
  local ARTIFACT_ID=$1
  local ARTIFACT_PATH=$2
  mvn -e -X --settings "$SCRIPT_DIR"/mvn_settings.xml deploy:deploy-file  \
    -Dfile="$SCRIPT_DIR"/"$ARTIFACT_PATH" \
    -Dversion=0.0."$MINOR_VERSION" \
    -DgroupId=ai.zipline \
    -DartifactId="$ARTIFACT_ID" \
    -Dpackaging=jar \
    -DgeneratePom=true  \
    -Durl=$REPO_URL \
    -DrepositoryId=$REPO_ID
}

mvn_deploy spark_uber_2.11 spark/target/scala-2.11/spark-assembly-0.1.0-SNAPSHOT.jar
mvn_deploy online_2.11 online/target/scala-2.11/online_2.11-0.1.0-SNAPSHOT.jar
mvn_deploy api_2.11 api/target/scala-2.11/api_2.11-0.1.0-SNAPSHOT.jar
mvn_deploy aggregator_2.11 aggregator/target/scala-2.11/aggregator_2.11-0.1.0-SNAPSHOT.jar
mvn_deploy spark_embedded_2.11 spark/target-embedded/scala-2.11/spark_embedded-assembly-0.1.0-SNAPSHOT.jar

mvn_deploy online_2.13 online/target/scala-2.13/online_2.13-0.1.0-SNAPSHOT.jar
mvn_deploy api_2.13 api/target/scala-2.13/api_2.13-0.1.0-SNAPSHOT.jar
mvn_deploy aggregator_2.13 aggregator/target/scala-2.13/aggregator_2.13-0.1.0-SNAPSHOT.jar
