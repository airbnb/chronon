#!/usr/bin/env python3

import argparse
from datetime import datetime
import json
import os
import subprocess


MODE_ARGS = {
    'backfill': '--conf-path={conf_path} --end-date={ds}',
    'upload': '--conf-path={conf_path} --end-date={ds}',
    'streaming': '--conf-path={conf_path} --user-jar={user_jar}'
}
ROUTES = {
    'group_bys': {
        'upload': 'GroupByUpload',
        'backfill': 'GroupBy',
        'streaming': 'GroupByStreaming'  # TODO
    },
    'joins': {
        'backfill': 'Join',
        'streaming': 'JoinStreaming'  # TODO
    },
    'staging_queries': {
        'backfill': 'StagingQuery'
    }
}


def check_call(cmd):
    print("Running command: " + cmd)
    return subprocess.check_call(cmd, shell=True)


def check_output(cmd):
    print("Running command: " + cmd)
    return subprocess.check_output(cmd, shell=True).strip()


def download_only_once(url, path):
    should_download = True
    path = path.strip()
    if os.path.exists(path):
        remote_size = int(check_output("curl -sI " + url + " | grep -i Content-Length | awk '{print $2}'"))
        local_size = int(check_output("wc -c <" + path))
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
        self.context, self.conf_type, self.team = self.conf.split('/')[:3]
        possible_modes = ROUTES[self.conf_type].keys()
        assert args.mode in possible_modes, "Invalid mode:{} for conf:{} of type:{}, please choose from {}".format(
            args.mode, self.conf, self.conf_type, possible_modes)
        self.mode = args.mode
        self.ds = args.ds
        self.jar_path = jar_path
        self.args = args.args
        self.user_jar = args.user_jar

    def set_env(self):
        with open(os.path.join(self.repo, self.conf), 'r') as conf_file:
            conf_json = json.load(conf_file)
        with open(os.path.join(self.repo, 'teams.json'), 'r') as teams_file:
            teams_json = json.load(teams_file)
        app_name = conf_json['metaData']['name']
        # env priority conf.metaData.modeToEnvMap >> team.env >> default_team.env
        # default env & conf env are optional, team env is not.
        env = teams_json.get('default', {}).get(self.context, {}).get(self.mode, {})
        team_env = teams_json[self.team].get(self.context, {}).get(self.mode, {})
        conf_env = conf_json.get('metaData').get('modeToEnvMap', {}).get(self.mode, {})
        env.update(team_env)
        env.update(conf_env)
        env["APP_NAME"] = app_name
        print("Setting env variables:")
        for key, value in env.items():
            print("    " + key + "=" + value)
            os.environ[key] = value

    def run(self):
        self.set_env()
        additional_args = (MODE_ARGS[self.mode] + self.args).format(
            conf_path=self.conf, ds=self.ds, user_jar=self.user_jar)
        command = 'bash {script} --class ai.zipline.spark.{main} {jar} {args}'.format(
            script=os.path.join(self.repo, 'spark_submit.sh'),
            jar=self.jar_path,
            main=ROUTES[self.conf_type][self.mode],
            args=additional_args
        )
        check_call(command)


if __name__ == "__main__":
    today = datetime.today().strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser(description='Submit various kinds of zipline jobs')
    parser.add_argument('--conf', required=True)
    parser.add_argument('--mode', choices=['backfill', 'streaming', 'upload'], default='backfill')
    parser.add_argument('--ds', default=today)
    parser.add_argument('--args', help='quoted string of any relevant additional args', default='')
    parser.add_argument('--repo', help='Path to zipline repo', default=os.getenv('ZIPLINE_REPO_PATH', '.'))
    parser.add_argument('--user_jar', help='Jar containing KvStore & Deserializer Impl', default=None)
    parser.add_argument('--version', help='Zipline version to use.', default="0.0.14")
    args = parser.parse_args()
    Runner(args, download_jar(args.version)).run()
