#!/usr/bin/env bash

# Steps to setup gcloud push ability
# Download gcloud cli and unpack: https://cloud.google.com/sdk/docs/install-sdk#installing_the_latest_version
# Install gcloud cli: ./google-cloud-sdk/install.sh
# Connect your account: ./google-cloud-sdk/bin/gcloud init
# Tell Nikhil to run this for permissions: gsutil iam ch user:<you email>@gmail.com:objectCreator,objectViewer gs://chronon


gsutil -m -o GSUtil:parallel_process_count=1 -o GSUtil:parallel_thread_count=24 cp -r releases/* gs://chronon