#!/usr/bin/env python3

import argparse
import json
import os
import re
import subprocess
from datetime import datetime
import xml.etree.ElementTree as ET

ONLINE_ARGS = '--online-jar={online_jar} --online-class={online_class}'
OFFLINE_ARGS = '--conf-path={conf_path} --end-date={ds}'
ONLINE_WRITE_ARGS = '--conf-path={conf_path} ' + ONLINE_ARGS
MODE_ARGS = {
    'backfill': OFFLINE_ARGS,
    'upload': OFFLINE_ARGS,
    'streaming': ONLINE_WRITE_ARGS,
    'metadata-upload': ONLINE_WRITE_ARGS,
    'fetch': ONLINE_ARGS,
    'local-streaming': ONLINE_WRITE_ARGS + ' -d'
}

ROUTES = {
    'group_bys': {
        'upload': 'group-by-upload',
        'backfill': 'group-by-backfill',
        'streaming': 'group-by-streaming',
        'local-streaming': 'group-by-streaming'
    },
    'joins': {
        'backfill': 'join',
        'metadata-upload': 'metadata-upload'
    },
    'staging_queries': {
        'backfill': 'staging-query-backfill',
    },
}

APP_NAME_TEMPLATE = "zipline_{conf_type}_{mode}_{context}_{name}"


def check_call(cmd):
    print("Running command: " + cmd)
    return subprocess.check_call(cmd.split())


def check_output(cmd):
    print("Running command: " + cmd)
    return subprocess.check_output(cmd.split()).strip()


def download_only_once(url, path):
    should_download = True
    path = path.strip()
    if os.path.exists(path):
        content_output = check_output("curl -sI " + url).decode('utf-8')
        content_length = re.search("(Content-Length:\\s)(\\d+)", content_output)
        remote_size = int(content_length.group().split()[-1])
        local_size = int(check_output("wc -c " + path).split()[0])
        print("""Files sizes of {url} vs. {path}
    Remote size: {remote_size}
    Local size : {local_size}""".format(**locals()))
        if local_size == remote_size:
            print("Sizes match. Assuming its already downloaded.")
            should_download = False
        if should_download:
            print("Different file from remote at local: " + path + ". Re-downloading..")
            check_call('curl {} -o {} --connect-timeout 10'.format(url, path))
    else:
        print("No file at: " + path + ". Downloading..")
        check_call('curl {} -o {} --connect-timeout 10'.format(url, path))


# TODO(Open Sourcing) this should be hard coded to mavencentral path
BASE_URL = "https://artifactory.d.musta.ch/artifactory/maven-airbnb-releases/ai/zipline/spark_uber_2.11"


def find_latest_master_version():
    metadata_content = check_output("curl -s {}/maven-metadata.xml".format(BASE_URL))
    meta_tree = ET.fromstring(metadata_content)
    versions = [
        node.text
        for node in meta_tree.findall("./versioning/versions/")
        if re.search(r"^\d+\.\d+\.\d+$", node.text)
    ]
    return versions[-1]


def download_jar(version):
    jar_path = os.environ.get('ZIPLINE_JAR_PATH', None)
    if jar_path is None:
        if version is None:
            version = find_latest_master_version()
        jar_url = "{}/{}/spark_uber_2.11-{}.jar".format(BASE_URL, version, version)
        jar_path = os.path.join('/tmp', jar_url.split('/')[-1])
        download_only_once(jar_url, jar_path)
    return jar_path


class Runner:
    def __init__(self, args, jar_path):
        self.repo = args.repo
        self.conf = args.conf
        self.online_modes = ['streaming', 'metadata-upload', 'fetch', 'local-streaming']
        are_args_valid = (args.mode not in online_modes) or (args.online_class and args.online_jar)
        assert are_args_valid, "must specify online-jar and online-class for online modes."
        if args.mode != 'metadata-upload':
            self.context, self.conf_type, self.team, _ = self.conf.split('/')[-4:]
            possible_modes = ROUTES[self.conf_type].keys()
            assert args.mode in possible_modes, "Invalid mode:{} for conf:{} of type:{}, please choose from {}".format(
                args.mode, self.conf, self.conf_type, possible_modes)
        self.mode = args.mode
        self.ds = args.ds
        self.jar_path = jar_path
        self.args = args.args
        self.online_jar = args.online_jar
        self.online_class = args.online_class
        self.app_name = args.app_name
        if self.mode == 'streaming':
            self.spark_submit = args.spark_streaming_submit_path
        else:
            self.spark_submit = args.spark_submit_path

    def set_env(self):
        with open(os.path.join(self.repo, self.conf), 'r') as conf_file:
            conf_json = json.load(conf_file)
        with open(os.path.join(self.repo, 'teams.json'), 'r') as teams_file:
            teams_json = json.load(teams_file)

        if not self.app_name:
            self.app_name = APP_NAME_TEMPLATE.format(
                mode=self.mode,
                conf_type=self.conf_type,
                context=self.context,
                name=conf_json['metaData']['name'])

        # env priority conf.metaData.modeToEnvMap >> team.env >> default_team.env
        # default env & conf env are optional, team env is not.
        env = teams_json.get('default', {}).get(self.context, {}).get(self.mode, {})
        team_env = teams_json[self.team].get(self.context, {}).get(self.mode, {})
        conf_env = conf_json.get('metaData').get('modeToEnvMap', {}).get(self.mode, {})
        env.update(team_env)
        env.update(conf_env)
        env["APP_NAME"] = self.app_name
        print("Setting env variables:")
        for key, value in env.items():
            print("    " + key + "=" + value)
            os.environ[key] = value

    def run(self):
        final_args = (MODE_ARGS[self.mode] + ' ' + self.args).format(
            conf_path=self.conf, ds=self.ds, online_jar=self.online_jar, online_class=self.online_class)
        if self.mode in self.online_modes:
            command = 'java -cp {jar} ai.zipline.spark.Driver metadata-upload {args}'.format(
                jar=self.jar_path,
                args=final_args
            )
        else:
            self.set_env()
            command = 'bash {script} --class ai.zipline.spark.Driver {jar} {subcommand} {args}'.format(
                script=self.spark_submit,
                jar=self.jar_path,
                subcommand=ROUTES[self.conf_type][self.mode],
                args=final_args
            )
        check_call(command)


if __name__ == "__main__":
    today = datetime.today().strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser(description='Submit various kinds of zipline jobs')
    zipline_repo_path = os.getenv('ZIPLINE_REPO_PATH', '.')
    parser.add_argument('--conf', required=True)
    parser.add_argument('--mode', choices=MODE_ARGS.keys(), default='backfill')
    parser.add_argument('--ds', default=today)
    parser.add_argument('--app_name', help='app name. Default to {}'.format(APP_NAME_TEMPLATE), default=None)
    parser.add_argument('--args', help='quoted string of any relevant additional args', default='')
    parser.add_argument('--repo', help='Path to zipline repo', default=zipline_repo_path)
    parser.add_argument('--online_jar',
                        help='Jar containing Online KvStore & Deserializer Impl. ' +
                             'Used for streaming and metadata-upload mode.',
                        default=os.environ.get('ZIPLINE_ONLINE_JAR', None))
    parser.add_argument('--online_class',
                        help='Class name of Online Impl. Used for streaming and metadata-upload mode.',
                        default=os.environ.get('ZIPLINE_ONLINE_CLASS', None))
    parser.add_argument('--version', help='Zipline version to use.', default=None)
    parser.add_argument('--spark-submit-path',
                        help='Path to spark-submit',
                        default=os.path.join(zipline_repo_path, 'scripts/spark_submit.sh'))
    parser.add_argument('--spark-streaming-submit-path',
                        help='Path to spark-submit for streaming',
                        default=os.path.join(zipline_repo_path, 'scripts/spark_streaming.sh'))
    parser.add_argument('--online-jar-fetch',
                        help='Path to script that can pull online jar. ' +
                             'This will run only when a file doesn\'t exist at location specified by online_jar',
                        default=os.path.join(zipline_repo_path, 'scripts/fetch_online_jar.sh'))

    args = parser.parse_args()
    Runner(args, download_jar(args.version)).run()
