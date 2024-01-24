#!/usr/bin/env python3

# ****************** NOTE **********************
# This script is used for checking if streaming jobs
# are alive in a yarn environment. If a job is already live,
# run.py will exit. If not it will launch a new streaming job.
# This behavior is intended for use from periodic airflow
# tasks, that try to keep the streaming job alive.
# CHECK FOR 'TODO'-s in this file.
# **********************************************



#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import json
import os
import subprocess
from shlex import split
import sys


ACTIVE_APP_STATUS = ['SUBMITTED', 'ACCEPTED', 'RUNNING']

# TODO: REPLACE WITH path to your YARN
YARN_EXECUTABLE = '/usr/bin/yarn'

# TODO: REPLACE WITH path to your cluster confs
EMR_HADOOP_CONF_PATH_TEMPLATE = '/etc/emr/{emr_cluster}/hadoop/conf'

YARN_TIMEOUT_SECONDS = 120


# returns list of active yarn apps on a cluster
def get_active_applications(
        emr_cluster,
        yarn_executable=YARN_EXECUTABLE,
        timeout=YARN_TIMEOUT_SECONDS,
        ssh_command=None):
    hadoop_conf = EMR_HADOOP_CONF_PATH_TEMPLATE.format(emr_cluster=emr_cluster)
    emr_application_list_cmd = '{yarn_executable} --config {hadoop_conf} application -list'.format(
            hadoop_conf=hadoop_conf,
            yarn_executable=yarn_executable
        )
    if ssh_command:
        emr_application_list_cmd = "{} {}".format(ssh_command, emr_application_list_cmd)
    print("Running yarn list command: {}".format(emr_application_list_cmd))
    output = subprocess.check_output(split(emr_application_list_cmd), timeout=timeout)
    if isinstance(output, bytes):
        output = output.decode()
    yarn_apps = []
    for app_listing in output.strip().split('\n')[2:]:
        tokens = [app_listing.strip() for app_listing in app_listing.split('\t')]
        app_id = tokens[0]
        job = {
            "app_id": app_id,
            "app_name": tokens[1],
            "type": tokens[2],
            "user": tokens[3],
            "queue": tokens[4],
            "status": tokens[5],
            "tracking_url": "{}/proxy/{}/".format(tokens[8], tokens[0]),
            "kill_cmd": '{yarn_executable} --config {hadoop_conf} application -kill {app_id}'.format(**locals())
        }
        yarn_apps.append(job)
    result = []
    for app in yarn_apps:
        if app["status"] in ACTIVE_APP_STATUS:
            app_json = json.dumps(app)
            print(app_json)
            result.append(app_json)
    return result


if __name__ == "__main__":
    """
    if len(sys.argv) < 2:
        # comes from teams.json
        cluster = os.environ.get("EMR_CLUSTER")
    else:
        cluster = sys.argv[1]
    assert cluster is not None, "cluster needs to be set either via $EMR_CLUSTER or via cli"
    get_active_applications(cluster)
    """
    []
