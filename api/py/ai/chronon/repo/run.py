#!/usr/bin/env python3

import argparse
import json
import os
import re
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime

ONLINE_ARGS = '--online-jar={online_jar} --online-class={online_class} '
OFFLINE_ARGS = '--conf-path={conf_path} --end-date={ds} '
ONLINE_WRITE_ARGS = '--conf-path={conf_path} ' + ONLINE_ARGS
ONLINE_OFFLINE_WRITE_ARGS = OFFLINE_ARGS + ONLINE_ARGS
ONLINE_MODES = ['streaming', 'metadata-upload', 'fetch', 'local-streaming', 'consistency-metrics-upload']
SPARK_MODES = ['backfill', 'upload', 'streaming', 'consistency-metrics-upload', 'analyze']

MODE_ARGS = {
    'backfill': OFFLINE_ARGS,
    'upload': OFFLINE_ARGS,
    'analyze': OFFLINE_ARGS,
    'streaming': ONLINE_WRITE_ARGS,
    'metadata-upload': ONLINE_WRITE_ARGS,
    'fetch': ONLINE_ARGS,
    'consistency-metrics-upload': ONLINE_OFFLINE_WRITE_ARGS,
    'local-streaming': ONLINE_WRITE_ARGS + ' -d'
}

ROUTES = {
    'group_bys': {
        'upload': 'group-by-upload',
        'backfill': 'group-by-backfill',
        'streaming': 'group-by-streaming',
        'local-streaming': 'group-by-streaming',
        'fetch': 'fetch',
        'analyze': 'analyze'
    },
    'joins': {
        'backfill': 'join',
        'metadata-upload': 'metadata-upload',
        'fetch': 'fetch',
        'consistency-metrics-upload': 'consistency-metrics-upload',
        'analyze': 'analyze'
    },
    'staging_queries': {
        'backfill': 'staging-query-backfill',
    },
}

APP_NAME_TEMPLATE = "chronon_{conf_type}_{mode}_{context}_{name}"


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
        content_length = re.search("(content-length:\\s)(\\d+)", content_output.lower())
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


def download_jar(version, jar_type='uber', release_tag=None):
    # TODO(Open Sourcing) this should be hard coded to mavencentral path
    base_url = "https://s01.oss.sonatype.org/service/local/repositories/public/content/ai/chronon/spark_{}_2.11".format(
        jar_type)
    jar_path = os.environ.get('CHRONON_JAR_PATH', None)
    if jar_path is None:
        if version is None:
            metadata_content = check_output("curl -s {}/maven-metadata.xml".format(base_url))
            meta_tree = ET.fromstring(metadata_content)
            versions = [
                node.text
                for node in meta_tree.findall("./versioning/versions/")
                if re.search(
                    r"^\d+\.\d+\.\d+{}$".format('\_{}\d*'.format(release_tag) if release_tag else ''),
                    node.text
                )
            ]
            version = versions[-1]
        jar_url = "{base_url}/{version}/spark_{jar_type}_2.11-{version}-assembly.jar".format(
            base_url=base_url,
            version=version,
            jar_type=jar_type
        )
        jar_path = os.path.join('/tmp', jar_url.split('/')[-1])
        download_only_once(jar_url, jar_path)
    return jar_path


def set_env_dict(env_map):
    for key, value in env_map.items():
        if key in os.environ:
            print("Found " + key + "=" + os.environ.get(key))
        else:
            print("Setting " + key + "=" + value)
            os.environ[key] = value


def set_common_env(repo):
    with open(os.path.join(repo, 'teams.json'), 'r') as teams_file:
        teams_json = json.load(teams_file)
        env = teams_json.get('default', {}).get("common_env", {})
        set_env_dict(env)


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
            print("Downloaded jar to {}".format(self.online_jar))

        if self.conf:
            self.context, self.conf_type, self.team, _ = self.conf.split('/')[-4:]
            possible_modes = ROUTES[self.conf_type].keys()
            assert args.mode in possible_modes, "Invalid mode:{} for conf:{} of type:{}, please choose from {}".format(
                args.mode, self.conf, self.conf_type, possible_modes)
        else:
            self.conf_type = args.conf_type
        self.ds = args.ds
        self.jar_path = jar_path
        self.args = args.args if args.args else ''
        self.online_class = args.online_class
        self.app_name = args.app_name
        if self.mode == 'streaming':
            self.spark_submit = args.spark_streaming_submit_path
        else:
            self.spark_submit = args.spark_submit_path
        self.list_apps_cmd = args.list_apps

    def set_env(self):
        conf_path = os.path.join(self.repo, self.conf)
        with open(conf_path, 'r') as conf_file:
            conf_json = json.load(conf_file)
        with open(os.path.join(self.repo, 'teams.json'), 'r') as teams_file:
            teams_json = json.load(teams_file)

        if not self.app_name:
            self.app_name = APP_NAME_TEMPLATE.format(
                mode=self.mode,
                conf_type=self.conf_type,
                context=self.context,
                name=conf_json['metaData']['name'])

        # env priority already set >> conf.metaData.modeToEnvMap >> team.env >> default_team.env
        # default env & conf env are optional, team env is not.
        env = teams_json.get('default', {}).get(self.context, {}).get(self.mode, {})
        team_env = teams_json[self.team].get(self.context, {}).get(self.mode, {})
        conf_env = conf_json.get('metaData').get('modeToEnvMap', {}).get(self.mode, {})
        env.update(team_env)
        env.update(conf_env)
        env["APP_NAME"] = self.app_name
        env["CHRONON_CONF_PATH"] = conf_path
        env["CHRONON_DRIVER_JAR"] = self.jar_path
        if self.mode in ONLINE_MODES:
            env["CHRONON_ONLINE_JAR"] = self.online_jar
        print("Setting env variables:")
        set_env_dict(env)

    def run(self):
        base_args = MODE_ARGS[self.mode].format(
            conf_path=self.conf, ds=self.ds, online_jar=self.online_jar, online_class=self.online_class)
        final_args = base_args + ' ' + str(self.args)
        subcommand = ROUTES[self.conf_type][self.mode]
        if self.sub_help or (self.mode not in SPARK_MODES):
            command = 'java -cp {jar} ai.chronon.spark.Driver {subcommand} {args}'.format(
                jar=self.jar_path,
                args='--help' if self.sub_help else final_args,
                subcommand=subcommand
            )
        else:
            self.set_env()
            if self.mode in ('streaming'):
                print("Checking to see if a streaming job by the name {} already exists".format(self.app_name))
                running_apps = check_output("{}".format(self.list_apps_cmd)).decode("utf-8")
                filtered_apps = [app for app in running_apps.split('\n') if self.app_name in app]
                if len(filtered_apps) > 0:
                    print("Found running apps by the name {} in \n{}\n".format(
                        self.app_name, '\n'.join(filtered_apps)))
                    assert len(filtered_apps) == 1, "More than one found, please kill them all"
                    print("All good. No need to start a new app.")
                    return
            command = 'bash {script} --class ai.chronon.spark.Driver {jar} {subcommand} {args}'.format(
                script=self.spark_submit,
                jar=self.jar_path,
                subcommand=subcommand,
                args=final_args
            )
        check_call(command)


if __name__ == "__main__":
    today = datetime.today().strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser(description='Submit various kinds of chronon jobs')
    chronon_repo_path = os.getenv('CHRONON_REPO_PATH', '.')
    set_common_env(chronon_repo_path)
    parser.add_argument('--conf', required=False, help='Conf param - required for every mode except fetch')
    parser.add_argument('--mode', choices=MODE_ARGS.keys(), default='backfill')
    parser.add_argument('--ds', default=today)
    parser.add_argument('--app-name', help='app name. Default to {}'.format(APP_NAME_TEMPLATE), default=None)
    parser.add_argument('--repo', help='Path to chronon repo', default=chronon_repo_path)
    parser.add_argument('--online-jar',
                        help='Jar containing Online KvStore & Deserializer Impl. ' +
                             'Used for streaming and metadata-upload mode.',
                        default=os.environ.get('CHRONON_ONLINE_JAR', None))
    parser.add_argument('--online-class',
                        help='Class name of Online Impl. Used for streaming and metadata-upload mode.',
                        default=os.environ.get('CHRONON_ONLINE_CLASS', None))
    parser.add_argument('--version', help='Chronon version to use.', default=None)
    parser.add_argument('--spark-submit-path',
                        help='Path to spark-submit',
                        default=os.path.join(chronon_repo_path, 'scripts/spark_submit.sh'))
    parser.add_argument('--spark-streaming-submit-path',
                        help='Path to spark-submit for streaming',
                        default=os.path.join(chronon_repo_path, 'scripts/spark_streaming.sh'))
    parser.add_argument('--online-jar-fetch',
                        help='Path to script that can pull online jar. ' +
                             'This will run only when a file doesn\'t exist at location specified by online_jar',
                        default=os.path.join(chronon_repo_path, 'scripts/fetch_online_jar.py'))
    parser.add_argument('--sub-help', action='store_true', help='print help command of the underlying jar and exit')
    parser.add_argument('--conf-type', default='group_bys',
                        help='related to sub-help - no need to set unless you are not working with a conf')
    parser.add_argument('--online-args', default=os.getenv('CHRONON_ONLINE_ARGS', ''),
                        help='Basic arguments that need to be supplied to all online modes')
    parser.add_argument('--chronon-jar', default=None, help='Path to chronon OS jar')
    parser.add_argument('--release-tag', default=None, help='Use the latest jar for a particular tag.')
    parser.add_argument('--list-apps', default="python3 " + os.path.join(chronon_repo_path, "scripts/yarn_list.py"),
                        help='command/script to list running jobs on the scheduler')
    args, unknown_args = parser.parse_known_args()
    jar_type = 'embedded' if args.mode == 'local-streaming' else 'uber'
    extra_args = (' ' + args.online_args) if args.mode in ONLINE_MODES else ''
    args.args = ' '.join(unknown_args) + extra_args
    jar_path = args.chronon_jar if args.chronon_jar else download_jar(args.version, jar_type, args.release_tag)
    Runner(args, jar_path).run()
