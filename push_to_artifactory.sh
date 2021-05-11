#!/usr/bin/env bash

BRANCH=`git branch --show-current`
if [ $BRANCH != "master" ]; then
    echo -e "You are on $BRANCH branch. Switch to master."
    exit 1
fi
TAG=`git describe --tags`
VERSION=`git describe --tags | awk 'match($0, "release-zl-([0-9]+.[0-9]+.[0-9]+)$", m) {print m[1]}'`
if [ -z "$VERSION" ]
then
    echo -e "Could not validate version based on git tag. Make sure to create a tag with \n\ngit tag -a -m '<tag message>' release-zl-X.X.X\n\nExiting.."
    exit 1
else
    echo -e "version $VERSION will be pushed to zipline_all repository."
fi
printf "push $TAG to git.."
git push origin $TAG
sbt assembly
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
mvn -e -X --settings $SCRIPT_DIR/mvn_settings.xml deploy:deploy-file  -Dfile=$SCRIPT_DIR/spark/target/scala-2.11/zipline-spark.jar -Dversion=$VERSION -DgroupId=ai.zipline -Dpackaging=jar -DgeneratePom=true  -Durl=https://artifactory.d.musta.ch/artifactory/maven-airbnb-releases -DrepositoryId=airbnb -DartifactId=zipline_all_2.11
