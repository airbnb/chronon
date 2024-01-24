#!/usr/bin/env python3

# TODO: Implement how to fetch your jar for online implementation of ai.chronon.online.Api interface/trait
# For example some companies host their jars on artifactory and you can pull from there 
# We expect the output of this script to be a string path to where the file is downloaded



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

from urllib.request import urlretrieve
import os
import logging


def download():
    output = "/tmp/company_chronon_online_jar.jar"
    logging.debug("Checking for existing JAR at {output}".format(**locals()))
    if not os.path.exists(output):
        download_url = " TODO: http://url.to.your.pkg.manager/your_package_name"
        logging.info("downloading from : {download_url}".format(**locals()))
        urlretrieve(download_url, filename=output)
        assert os.path.exists(output)
        logging.info("Finished downloading to: {output}".format(**locals()))
    return output


if __name__ == "__main__":
    # In the sample case we put this jar in a specific docker location else use print(download()):
    print("/srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar")
