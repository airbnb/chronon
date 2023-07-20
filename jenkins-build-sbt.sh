#! /usr/bin/env bash

. /usr/stripe/bin/docker/stripe-init-build

# The steps in this file largely follow the commands in .circleci/config.yml

# Build the Scala code
# Increase if we see OOMs
export SBT_OPTS="-XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=4G -Xmx4G -Xms2G"

# Set inside CI 
export INSIDE_CI=true

# Grab current project version (stripping out the -SNAPSHOT)
# We manually override the version.sbt instead of using the sbt-release plugin for
# our Stripe customizations to continue using the upstream versioning as a base
current_version=`cat version.sbt | cut -d '"' -f2 | cut -d '-' -f1`
git_sha=$(git rev-parse --short HEAD)
date_str=`date "+%Y%m%d"`

echo "Current Chronon version: $current_version; we are on git sha: $git_sha"
release_version="$current_version-$date_str-$git_sha"
echo "Setting version to $release_version for release."
echo "version := \"$release_version\"" > version.sbt

echo "Kicking off Chronon sbt build & tests"
sbt "++ 2.12.12 test"
echo "Kicking off Chronon artifactory publish"
export CHRONON_SNAPSHOT_REPO='https://artifactory-content.stripe.build/artifactory/maven-snapshots-local'
sbt "++ 2.12.12 publish"

# Create build directory
mkdir -p /src/build
# Copy build files to /src/build so they can be accessed by the artifact publish box
cp -rv api/py/dist/* /src/build
