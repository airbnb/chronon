#!/bin/bash

set -euo pipefail

# Print usage if no version is passed
if [[ $# -lt 1 || -z "${1:-}" ]]; then
  echo "❌ VERSION not provided."
  echo "Usage: $0 <version>"
  echo "Example: $0 0.0.100"
  exit 1
fi

VERSION="$1"

PROJECTS=(api aggregator online spark_uber spark_embedded service)
SCALAS=(2.11 2.12 2.13)

# Iterate over main project combinations
for project in "${PROJECTS[@]}"; do
  for scala in "${SCALAS[@]}"; do
    PACKAGE="${project}_${scala}"
    echo "▶️ Uploading $PACKAGE for version $VERSION..."
    bash maven_upload.sh "$VERSION" "$PACKAGE"
  done
done

# Add the special case
PACKAGE="flink_2.12"
echo "▶️ Uploading $PACKAGE for version $VERSION..."
bash maven_upload.sh "$VERSION" "$PACKAGE"

echo "✅ All uploads attempted."