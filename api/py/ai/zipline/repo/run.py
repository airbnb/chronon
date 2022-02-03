#!/usr/bin/env python3

import argparse
import json
import os
import re
import subprocess
from datetime import datetime

MODE_ARGS = {
    'backfill': '--conf-path={conf_path} --end-date={ds}',
    'upload': '--conf-path={conf_path} --end-date={ds}',
    'streaming': '--conf-path={conf_path} --online-jar={online_jar} --online-class={online_class}',
    'metadata-upload': '--conf-path={conf_path} --online-jar={online_jar} --online-class={online_class}',
}

ONLINE_JAR_PATH_ENV_VAR = "ZIPLINE_ONLINE_JAR_PATH"
ONLINE_CLASS_ENV_VAR = "ZIPLINE_ONLINE_CLASS"
DEFAULT_ARGS_ENV_VAR = "ZIPLINE_DEFAULT_ARGS"

ROUTES = {
    'group_bys': {
        'upload': 'group-by-upload',
        'backfill': 'group-by-backfill',
        'streaming': 'group-by-streaming',
    },
    'joins': {
        'backfill': 'join',
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


def download_jar(version):
    jar_path = os.environ.get('ZIPLINE_JAR_PATH', None)
    if jar_path is None:
        # TODO(Open Sourcing) this should be hard coded to mavencentral path
        jar_url = "https://artifactory.d.musta.ch/artifactory/maven-airbnb-releases/ai/zipline/" \
                  "spark_uber_2.11/{}/spark_uber_2.11-{}.jar".format(version, version)
        jar_path = os.path.join('/tmp', jar_url.split('/')[-1])
        download_only_once(jar_url, jar_path)
    return jar_path


class Runner:
    def __init__(self, args, jar_path):
        self.repo = args.repo
        self.conf = args.conf
        assert(args.mode not in ['streaming', 'metadata-upload'] or args.online_class and args.online_jar,
               "must specify online-jar and online-class for streaming mode or metadata-upload mode")
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
        if self.mode == 'metadata-upload':
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
    parser.add_argument('--args', help='quoted string of any relevant additional args, defaults to the `ZIPLINE_DEFAULT_ARGS` environment variable.', 
                        default=os.getenv(DEFAULT_ARGS_ENV_VAR, ""))
    parser.add_argument('--repo', help='Path to zipline repo', default=zipline_repo_path)
    parser.add_argument('--online_jar',
                        help='Jar containing Online KvStore & Deserializer Impl, defaults to the `ZIPLINE_ONLINE_JAR` environment variable.'
                        'Used for streaming and metadata-upload mode.', default=os.getenv(ONLINE_JAR_PATH_ENV_VAR))
    parser.add_argument('--online_class',
                        help='Class name of Online Impl. Used for streaming and metadata-upload mode, defaults to the `ZIPLINE_ONLINE_CLASS` environment variable.',
                        default=os.getenv(ONLINE_CLASS_ENV_VAR))
    parser.add_argument('--version', help='Zipline version to use.', default="0.0.29")
    parser.add_argument('--spark-submit-path',
                        help='Path to spark-submit',
                        default=os.path.join(zipline_repo_path, 'scripts/spark_submit.sh'))
    parser.add_argument('--spark-streaming-submit-path',
                        help='Path to spark-submit for streaming',
                        default=os.path.join(zipline_repo_path, 'scripts/spark_streaming.sh'))

    args = parser.parse_args()
    Runner(args, download_jar(args.version)).run()
