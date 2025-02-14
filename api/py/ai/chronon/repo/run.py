#!/usr/bin/env python3
"""
run.py needs to only depend in python standard library to simplify execution requirements.
"""

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

import argparse
import json
import logging
import multiprocessing
import os
import re
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

ONLINE_ARGS = "--online-jar={online_jar} --online-class={online_class} "
OFFLINE_ARGS = "--conf-path={conf_path} --end-date={ds} "
ONLINE_WRITE_ARGS = "--conf-path={conf_path} " + ONLINE_ARGS
ONLINE_OFFLINE_WRITE_ARGS = OFFLINE_ARGS + ONLINE_ARGS
ONLINE_MODES = [
    "streaming",
    "metadata-upload",
    "fetch",
    "local-streaming",
]
SPARK_MODES = [
    "backfill",
    "backfill-left",
    "backfill-final",
    "upload",
    "streaming",
    "consistency-metrics-compute",
    "compare",
    "analyze",
    "stats-summary",
    "log-summary",
    "log-flattener",
    "metadata-export",
    "label-join",
]
MODES_USING_EMBEDDED = ["metadata-upload", "fetch", "local-streaming"]

# Constants for supporting multiple spark versions.
SUPPORTED_SPARK = ["2.4.0", "3.1.1", "3.2.1"]
SCALA_VERSION_FOR_SPARK = {"2.4.0": "2.11", "3.1.1": "2.12", "3.2.1": "2.13"}

MODE_ARGS = {
    "backfill": OFFLINE_ARGS,
    "backfill-left": OFFLINE_ARGS,
    "backfill-final": OFFLINE_ARGS,
    "upload": OFFLINE_ARGS,
    "stats-summary": OFFLINE_ARGS,
    "log-summary": OFFLINE_ARGS,
    "analyze": OFFLINE_ARGS,
    "streaming": ONLINE_WRITE_ARGS,
    "metadata-upload": ONLINE_WRITE_ARGS,
    "fetch": ONLINE_ARGS,
    "consistency-metrics-compute": OFFLINE_ARGS,
    "compare": OFFLINE_ARGS,
    "local-streaming": ONLINE_WRITE_ARGS + " -d",
    "log-flattener": OFFLINE_ARGS,
    "metadata-export": OFFLINE_ARGS,
    "label-join": OFFLINE_ARGS,
    "info": "",
}

ROUTES = {
    "group_bys": {
        "upload": "group-by-upload",
        "backfill": "group-by-backfill",
        "streaming": "group-by-streaming",
        "metadata-upload": "metadata-upload",
        "local-streaming": "group-by-streaming",
        "fetch": "fetch",
        "analyze": "analyze",
        "metadata-export": "metadata-export",
    },
    "joins": {
        "backfill": "join",
        "backfill-left": "join-left",
        "backfill-final": "join-final",
        "metadata-upload": "metadata-upload",
        "fetch": "fetch",
        "consistency-metrics-compute": "consistency-metrics-compute",
        "compare": "compare-join-query",
        "stats-summary": "stats-summary",
        "log-summary": "log-summary",
        "analyze": "analyze",
        "log-flattener": "log-flattener",
        "metadata-export": "metadata-export",
        "label-join": "label-join",
    },
    "staging_queries": {
        "backfill": "staging-query-backfill",
        "metadata-export": "metadata-export",
    },
}

UNIVERSAL_ROUTES = ["info"]

APP_NAME_TEMPLATE = "chronon_{conf_type}_{mode}_{context}_{name}"
RENDER_INFO_DEFAULT_SCRIPT = "scripts/render_info.py"


def retry_decorator(retries=3, backoff=20):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            attempt = 0
            while attempt <= retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    logging.exception(e)
                    sleep_time = attempt * backoff
                    logging.info(
                        "[{}] Retry: {} out of {}/ Sleeping for {}".format(func.__name__, attempt, retries, sleep_time)
                    )
                    time.sleep(sleep_time)
            return func(*args, **kwargs)

        return wrapped

    return wrapper


def custom_json(conf):
    """Extract the json stored in customJson for a conf."""
    if conf.get("metaData", {}).get("customJson"):
        return json.loads(conf["metaData"]["customJson"])
    return {}


def check_call(cmd):
    print("Running command: " + cmd)
    return subprocess.check_call(cmd.split(), bufsize=0)


def check_output(cmd):
    print("Running command: " + cmd)
    return subprocess.check_output(cmd.split(), stderr=subprocess.STDOUT, bufsize=0).strip()


def download_only_once(url, path, skip_download=False):
    if skip_download:
        print("Skipping download of " + path)
        return
    should_download = True
    path = path.strip()
    if os.path.exists(path):
        content_output = check_output("curl -sI " + url).decode("utf-8")
        content_length = re.search("(content-length:\\s)(\\d+)", content_output.lower())
        remote_size = int(content_length.group().split()[-1])
        local_size = int(check_output("wc -c " + path).split()[0])
        print(
            """Files sizes of {url} vs. {path}
    Remote size: {remote_size}
    Local size : {local_size}""".format(
                **locals()
            )
        )
        if local_size == remote_size:
            print("Sizes match. Assuming its already downloaded.")
            should_download = False
        if should_download:
            print("Different file from remote at local: " + path + ". Re-downloading..")
            check_call("curl {} -o {} --connect-timeout 10".format(url, path))
    else:
        print("No file at: " + path + ". Downloading..")
        check_call("curl {} -o {} --connect-timeout 10".format(url, path))


@retry_decorator(retries=3, backoff=50)
def download_jar(
    version,
    jar_type="uber",
    release_tag=None,
    spark_version="3.1.1",
    skip_download=False,
):
    assert (
        spark_version in SUPPORTED_SPARK
    ), f"Received unsupported spark version {spark_version}. Supported spark versions are {SUPPORTED_SPARK}"
    scala_version = SCALA_VERSION_FOR_SPARK[spark_version]
    maven_url_prefix = os.environ.get("CHRONON_MAVEN_MIRROR_PREFIX", None)
    default_url_prefix = "https://s01.oss.sonatype.org/service/local/repositories/public/content"
    url_prefix = maven_url_prefix if maven_url_prefix else default_url_prefix
    base_url = "{}/ai/chronon/spark_{}_{}".format(url_prefix, jar_type, scala_version)
    print("Downloading jar from url: " + base_url)
    jar_path = os.environ.get("CHRONON_DRIVER_JAR", None)
    if jar_path is None:
        if version == "latest":
            version = None
        if version is None:
            metadata_content = check_output("curl -s {}/maven-metadata.xml".format(base_url))
            meta_tree = ET.fromstring(metadata_content)
            versions = [
                node.text
                for node in meta_tree.findall("./versioning/versions/")
                if re.search(
                    r"^\d+\.\d+\.\d+{}$".format("\_{}\d*".format(release_tag) if release_tag else ""),
                    node.text,
                )
            ]
            version = versions[-1]
        jar_url = "{base_url}/{version}/spark_{jar_type}_{scala_version}-{version}-assembly.jar".format(
            base_url=base_url,
            version=version,
            scala_version=scala_version,
            jar_type=jar_type,
        )
        jar_path = os.path.join("/tmp", jar_url.split("/")[-1])
        download_only_once(jar_url, jar_path, skip_download)
    return jar_path


def set_runtime_env(args):
    """
    Setting the runtime environment variables.
    These are extracted from the common env, the team env and the common env.
    In order to use the environment variables defined in the configs as overrides for the args in the cli this method
    needs to be run before the runner and jar downloads.

    The order of priority is:
        - Environment variables existing already.
        - Environment variables derived from args (like app_name)
        - conf.metaData.modeToEnvMap for the mode (set on config)
        - team's dev environment for each mode set on teams.json
        - team's prod environment for each mode set on teams.json
        - default team environment per context and mode set on teams.json
        - Common Environment set in teams.json
    """
    environment = {
        "common_env": {},
        "conf_env": {},
        "default_env": {},
        "team_env": {},
        "production_team_env": {},
        "cli_args": {},
    }
    conf_type = None
    # Normalize modes that are effectively replacement of each other (streaming/local-streaming)
    effective_mode = args.mode
    if effective_mode and "streaming" in effective_mode:
        effective_mode = "streaming"
    if args.repo:
        teams_file = os.path.join(args.repo, "teams.json")
        if os.path.exists(teams_file):
            with open(teams_file, "r") as infile:
                teams_json = json.load(infile)
            environment["common_env"] = teams_json.get("default", {}).get("common_env", {})
            if args.conf and effective_mode:
                try:
                    _, conf_type, team, _ = args.conf.split("/")[-4:]
                except Exception as e:
                    logging.error(
                        "Invalid conf path: {}, please ensure to supply the relative path to zipline/ folder".format(
                            args.conf
                        )
                    )
                    raise e
                if not team:
                    team = "default"
                # context is the environment in which the job is running, which is provided from the args,
                # default to be dev.
                if args.env:
                    context = args.env
                else:
                    context = "dev"
                logging.info(f"Context: {context} -- conf_type: {conf_type} -- team: {team}")
                conf_path = os.path.join(args.repo, args.conf)
                if os.path.isfile(conf_path):
                    with open(conf_path, "r") as conf_file:
                        conf_json = json.load(conf_file)
                    environment["conf_env"] = conf_json.get("metaData").get("modeToEnvMap", {}).get(effective_mode, {})
                    # Load additional args used on backfill.
                    if custom_json(conf_json) and effective_mode in ["backfill", "backfill-left", "backfill-final"]:
                        environment["conf_env"]["CHRONON_CONFIG_ADDITIONAL_ARGS"] = " ".join(
                            custom_json(conf_json).get("additional_args", [])
                        )
                    environment["cli_args"]["APP_NAME"] = APP_NAME_TEMPLATE.format(
                        mode=effective_mode,
                        conf_type=conf_type,
                        context=context,
                        name=conf_json["metaData"]["name"],
                    )
                environment["team_env"] = teams_json[team].get(context, {}).get(effective_mode, {})
                # fall-back to prod env even in dev mode when dev env is undefined.
                environment["production_team_env"] = teams_json[team].get("production", {}).get(effective_mode, {})
                # By default use production env.
                environment["default_env"] = teams_json.get("default", {}).get("production", {}).get(effective_mode, {})
                environment["cli_args"]["CHRONON_CONF_PATH"] = conf_path
    if args.app_name:
        environment["cli_args"]["APP_NAME"] = args.app_name
    else:
        if not args.app_name and not environment["cli_args"].get("APP_NAME"):
            # Provide basic app_name when no conf is defined.
            # Modes like metadata-upload and metadata-export can rely on conf-type or folder rather than a conf.
            environment["cli_args"]["APP_NAME"] = "_".join(
                [
                    k
                    for k in [
                        "chronon",
                        conf_type,
                        args.mode.replace("-", "_") if args.mode else None,
                    ]
                    if k is not None
                ]
            )

    # Adding these to make sure they are printed if provided by the environment.
    environment["cli_args"]["CHRONON_DRIVER_JAR"] = args.chronon_jar
    environment["cli_args"]["CHRONON_ONLINE_JAR"] = args.online_jar
    environment["cli_args"]["CHRONON_ONLINE_CLASS"] = args.online_class
    order = [
        "conf_env",
        "team_env",
        "production_team_env",
        "default_env",
        "common_env",
        "cli_args",
    ]
    print("Setting env variables:")
    for key in os.environ:
        if any([key in environment[set_key] for set_key in order]):
            print(f"From <environment> found {key}={os.environ[key]}")
    for set_key in order:
        for key, value in environment[set_key].items():
            if key not in os.environ and value is not None:
                print(f"From <{set_key}> setting {key}={value}")
                os.environ[key] = value


class Runner:
    def __init__(self, args, jar_path):
        self.repo = args.repo
        self.conf = args.conf
        self.sub_help = args.sub_help
        self.mode = args.mode
        self.online_jar = args.online_jar
        valid_jar = args.online_jar and os.path.exists(args.online_jar)
        # fetch online jar if necessary
        if (self.mode in ONLINE_MODES) and (not args.sub_help) and not valid_jar:
            print("Downloading online_jar")
            self.online_jar = check_output("{}".format(args.online_jar_fetch)).decode("utf-8")
            os.environ["CHRONON_ONLINE_JAR"] = self.online_jar
            print("Downloaded jar to {}".format(self.online_jar))

        if self.conf:
            try:
                self.context, self.conf_type, self.team, _ = self.conf.split("/")[-4:]
            except Exception as e:
                logging.error(
                    "Invalid conf path: {}, please ensure to supply the relative path to zipline/ folder".format(
                        self.conf
                    )
                )
                raise e
            possible_modes = list(ROUTES[self.conf_type].keys()) + UNIVERSAL_ROUTES
            assert args.mode in possible_modes, "Invalid mode:{} for conf:{} of type:{}, please choose from {}".format(
                args.mode, self.conf, self.conf_type, possible_modes
            )
        else:
            self.conf_type = args.conf_type
        self.ds = args.end_ds if hasattr(args, "end_ds") and args.end_ds else args.ds
        self.start_ds = args.start_ds if hasattr(args, "start_ds") and args.start_ds else None
        self.parallelism = int(args.parallelism) if hasattr(args, "parallelism") and args.parallelism else 1
        self.jar_path = jar_path
        self.args = args.args if args.args else ""
        self.online_class = args.online_class
        self.app_name = args.app_name
        if self.mode == "streaming":
            self.spark_submit = args.spark_streaming_submit_path
        elif self.mode == "info":
            assert os.path.exists(args.render_info), "Invalid path for the render info script: {}".format(
                args.render_info
            )
            self.render_info = args.render_info
        else:
            self.spark_submit = args.spark_submit_path
        self.list_apps_cmd = args.list_apps

    def run(self):
        command_list = []
        if self.mode == "info":
            command_list.append(
                "python3 {script} --conf {conf} --ds {ds} --repo {repo}".format(
                    script=self.render_info, conf=self.conf, ds=self.ds, repo=self.repo
                )
            )
        elif self.sub_help or (self.mode not in SPARK_MODES):
            command_list.append(
                "java -cp {jar} ai.chronon.spark.Driver {subcommand} {args}".format(
                    jar=self.jar_path,
                    args="--help" if self.sub_help else self._gen_final_args(),
                    subcommand=ROUTES[self.conf_type][self.mode],
                )
            )
        else:
            if self.mode in ["streaming"]:
                # streaming mode
                print("Checking to see if a streaming job by the name {} already exists".format(self.app_name))
                running_apps = []
                try:
                    running_apps = check_output("{}".format(self.list_apps_cmd)).decode("utf-8").split("\n")
                except subprocess.CalledProcessError as e:
                    print("Failed to retrieve running apps. Error:")
                    print(e.output.decode("utf-8"))
                    raise e
                running_app_map = {}
                for app in running_apps:
                    try:
                        app_json = json.loads(app.strip())
                        app_name = app_json["app_name"].strip()
                        if app_name not in running_app_map:
                            running_app_map[app_name] = []
                        running_app_map[app_name].append(app_json)
                    except Exception as ex:
                        print("failed to process line into app: " + app)
                        print(ex)

                filtered_apps = running_app_map.get(self.app_name, [])
                if len(filtered_apps) > 0:
                    print(
                        "Found running apps by the name {} in \n{}\n".format(
                            self.app_name,
                            "\n".join([str(app) for app in filtered_apps]),
                        )
                    )
                    if self.mode == "streaming":
                        assert len(filtered_apps) == 1, "More than one found, please kill them all"
                        print("All good. No need to start a new app.")
                        return
                command = (
                    "bash {script} --class ai.chronon.spark.Driver {jar} {subcommand} {args} {additional_args}"
                ).format(
                    script=self.spark_submit,
                    jar=self.jar_path,
                    subcommand=ROUTES[self.conf_type][self.mode],
                    args=self._gen_final_args(),
                    additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
                )
                command_list.append(command)
            else:
                # offline mode
                if self.parallelism > 1:
                    assert self.start_ds is not None and self.ds is not None, (
                        "To use parallelism, please specify --start-ds and --end-ds to "
                        "break down into multiple backfill jobs"
                    )
                    date_ranges = split_date_range(self.start_ds, self.ds, self.parallelism)
                    for start_ds, end_ds in date_ranges:
                        command = (
                            "bash {script} --class ai.chronon.spark.Driver {jar} {subcommand} {args} {additional_args}"
                        ).format(
                            script=self.spark_submit,
                            jar=self.jar_path,
                            subcommand=ROUTES[self.conf_type][self.mode],
                            args=self._gen_final_args(start_ds=start_ds, end_ds=end_ds),
                            additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
                        )
                        command_list.append(command)
                else:
                    command = (
                        "bash {script} --class ai.chronon.spark.Driver {jar} {subcommand} {args} {additional_args}"
                    ).format(
                        script=self.spark_submit,
                        jar=self.jar_path,
                        subcommand=ROUTES[self.conf_type][self.mode],
                        args=self._gen_final_args(self.start_ds),
                        additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
                    )
                    command_list.append(command)
        if len(command_list) > 1:
            # parallel backfill mode
            with multiprocessing.Pool(processes=int(self.parallelism)) as pool:
                logging.info("Running args list {} with pool size {}".format(command_list, self.parallelism))
                pool.map(check_call, command_list)
        elif len(command_list) == 1:
            check_call(command_list[0])

    def _gen_final_args(self, start_ds=None, end_ds=None):
        base_args = MODE_ARGS[self.mode].format(
            conf_path=self.conf,
            ds=end_ds if end_ds else self.ds,
            online_jar=self.online_jar,
            online_class=self.online_class,
        )
        override_start_partition_arg = " --start-partition-override=" + start_ds if start_ds else ""
        final_args = base_args + " " + str(self.args) + override_start_partition_arg
        return final_args


def split_date_range(start_date, end_date, parallelism):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    if start_date > end_date:
        raise ValueError("Start date should be earlier than end date")
    total_days = (end_date - start_date).days + 1  # +1 to include the end_date in the range

    # Check if parallelism is greater than total_days
    if parallelism > total_days:
        raise ValueError("Parallelism should be less than or equal to total days")

    split_size = total_days // parallelism
    date_ranges = []

    for i in range(parallelism):
        split_start = start_date + timedelta(days=i * split_size)
        if i == parallelism - 1:
            split_end = end_date
        else:
            split_end = split_start + timedelta(days=split_size - 1)
        date_ranges.append((split_start.strftime("%Y-%m-%d"), split_end.strftime("%Y-%m-%d")))
    return date_ranges


def set_defaults(parser):
    """Set default values based on environment"""
    chronon_repo_path = os.environ.get("CHRONON_REPO_PATH", ".")
    today = datetime.today().strftime("%Y-%m-%d")
    parser.set_defaults(
        mode="backfill",
        ds=today,
        app_name=os.environ.get("APP_NAME"),
        online_jar=os.environ.get("CHRONON_ONLINE_JAR"),
        repo=chronon_repo_path,
        online_class=os.environ.get("CHRONON_ONLINE_CLASS"),
        version=os.environ.get("VERSION"),
        spark_version=os.environ.get("SPARK_VERSION", "3.1.1"),
        spark_submit_path=os.path.join(chronon_repo_path, "scripts/spark_submit.sh"),
        spark_streaming_submit_path=os.path.join(chronon_repo_path, "scripts/spark_streaming.sh"),
        online_jar_fetch=os.path.join(chronon_repo_path, "scripts/fetch_online_jar.py"),
        conf_type="group_bys",
        online_args=os.environ.get("CHRONON_ONLINE_ARGS", ""),
        chronon_jar=os.environ.get("CHRONON_DRIVER_JAR"),
        list_apps="python3 " + os.path.join(chronon_repo_path, "scripts/yarn_list.py"),
        render_info=os.path.join(chronon_repo_path, RENDER_INFO_DEFAULT_SCRIPT),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit various kinds of chronon jobs")
    parser.add_argument(
        "--conf",
        required=False,
        help="Conf param - required for every mode except fetch",
    )
    parser.add_argument(
        "--env",
        required=False,
        default="dev",
        help="Running environment - default to be dev",
    )
    parser.add_argument("--mode", choices=MODE_ARGS.keys())
    parser.add_argument("--ds", help="the end partition to backfill the data")
    parser.add_argument("--app-name", help="app name. Default to {}".format(APP_NAME_TEMPLATE))
    parser.add_argument(
        "--start-ds",
        help="override the original start partition for a range backfill. "
        "It only supports staging query, group by backfill and join jobs. "
        "It could leave holes in your final output table due to the override date range.",
    )
    parser.add_argument("--end-ds", help="the end ds for a range backfill")
    parser.add_argument(
        "--parallelism",
        help="break down the backfill range into this number of tasks in parallel. "
        "Please use it along with --start-ds and --end-ds and only in manual mode",
    )
    parser.add_argument("--repo", help="Path to chronon repo")
    parser.add_argument(
        "--online-jar",
        help="Jar containing Online KvStore & Deserializer Impl. " + "Used for streaming and metadata-upload mode.",
    )
    parser.add_argument(
        "--online-class",
        help="Class name of Online Impl. Used for streaming and metadata-upload mode.",
    )
    parser.add_argument("--version", help="Chronon version to use.")
    parser.add_argument("--spark-version", help="Spark version to use for downloading jar.")
    parser.add_argument("--spark-submit-path", help="Path to spark-submit")
    parser.add_argument("--spark-streaming-submit-path", help="Path to spark-submit for streaming")
    parser.add_argument(
        "--online-jar-fetch",
        help="Path to script that can pull online jar. "
        + "This will run only when a file doesn't exist at location specified by online_jar",
    )
    parser.add_argument(
        "--sub-help",
        action="store_true",
        help="print help command of the underlying jar and exit",
    )
    parser.add_argument(
        "--conf-type",
        help="related to sub-help - no need to set unless you are not working with a conf",
    )
    parser.add_argument(
        "--online-args",
        help="Basic arguments that need to be supplied to all online modes",
    )
    parser.add_argument("--chronon-jar", help="Path to chronon OS jar")
    parser.add_argument("--release-tag", help="Use the latest jar for a particular tag.")
    parser.add_argument("--list-apps", help="command/script to list running jobs on the scheduler")
    parser.add_argument(
        "--render-info",
        help="Path to script rendering additional information of the given config. "
        + "Only applicable when mode is set to info",
    )
    set_defaults(parser)
    pre_parse_args, _ = parser.parse_known_args()
    # We do a pre-parse to extract conf, mode, etc and set environment variables and re parse default values.
    set_runtime_env(pre_parse_args)
    set_defaults(parser)
    args, unknown_args = parser.parse_known_args()
    jar_type = "embedded" if args.mode in MODES_USING_EMBEDDED else "uber"
    extra_args = (" " + args.online_args) if args.mode in ONLINE_MODES else ""
    args.args = " ".join(unknown_args) + extra_args
    jar_path = (
        args.chronon_jar
        if args.chronon_jar
        else download_jar(
            args.version,
            jar_type=jar_type,
            release_tag=args.release_tag,
            spark_version=os.environ.get("SPARK_VERSION", args.spark_version),
        )
    )
    Runner(args, os.path.expanduser(jar_path)).run()
