#!/bin/bash

set -euxo pipefail
 
root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
 
cd "$root"

if [ "production" == "$STRIPE_ENVIRONMENT" ]; then
    base="artifactory"
else
    base="qa-artifactory"
fi

echo "Publishing Chronon Python package to $base"
 
# do not loop if there are no files
shopt -s nullglob
for f in /src/build/*; do
    echo " - uploading $f"
    "/opt/conda/envs/zipline_py/bin/twine" \
        upload "$f" \
        --skip-existing \
        --non-interactive \
        --username anonymous \
        --password '' \
        --client-cert /etc/ssl/private/machine-cert-and-key.pem \
        --repository-url https://"$base".stripe.build/artifactory/api/pypi/pypi-local-oss-forks/
done