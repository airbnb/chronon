#!/usr/bin/env python3

# TODO: Implement how to fetch your jar for online implementation of ai.chronon.online.Api interface/trait
# For example some companies host their jars on artifactory and you can pull from there 
# We expect the output of this script to be a string path to where the file is downloaded


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
    print(download())