#!/usr/bin/env python3

import requests

SPARK_FUNCTION_DEFINITION_FILE = "https://raw.githubusercontent.com/apache/spark/master/python/pyspark/sql/functions.py"

if __name__ == "__main__":
    response = requests.get(SPARK_FUNCTION_DEFINITION_FILE)
    print_signature_line = False
    for line in response.content.splitlines():
        if print_signature_line:
            print(line)
            print_signature_line = False
        if line.startswith(b'@try_remote_functions'):
            print_signature_line = True
