#!/usr/bin/env bash

set -euxo pipefail

git checkout master && git pull
NEW_TAG=$(git tag | tail -n 1 | awk -F. -v OFS=. 'NF==1{print ++$NF}; NF>1{if(length($NF+1)>length($NF))$(NF-1)++; $NF=sprintf("%0*d", length($NF), ($NF+1)%(10^length($NF))); print}')
BRANCH_GIT=$(git branch --show-current)
BRANCH_GIT_SHA=$(git log --pretty=format:'%h' -n 1)
echo "Tagging $BRANCH_GIT@$BRANCH_GIT_SHA with $NEW_TAG (minor version incremented)"
#git tag -a -m '' release-zl-X.X.X
#git push origin $NEW_TAG
VERSION=$(git describe --tags| sed -E "s/release-zl-([0-9]+.[0-9]+.[0-9]+).*/\1/")

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "Building jars"
sbt assembly

# releasing fat jar
mvn -e -X --settings $SCRIPT_DIR/mvn_settings.xml deploy:deploy-file  -Dfile=$SCRIPT_DIR/spark/target/scala-2.11/zipline-spark.jar -Dversion=$VERSION -DgroupId=ai.zipline -DartifactId=zipline_all_2.11 -Dpackaging=jar -DgeneratePom=true  -Durl=https://artifactory.d.musta.ch/artifactory/maven-airbnb-releases -DrepositoryId=airbnb
