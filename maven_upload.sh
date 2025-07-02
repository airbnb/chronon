#!/usr/bin/env bash
set -euo pipefail

# Check for required arguments
if [[ $# -lt 2 ]]; then
  echo "❌ Missing arguments."
  echo "Usage: $0 <VERSION> <PACKAGE_NAME>"
  echo "Example: $0 0.0.100 aggregator_2.13"
  exit 1
fi

# Input arguments
VERSION="$1"
PACKAGE="$2"

# Validate PACKAGE format
if [[ ! "$PACKAGE" =~ ^((api|aggregator|online|spark_uber|spark_embedded|service)_(2\.11|2\.12|2\.13)|flink_2\.12)$ ]]; then
  echo "❌ Invalid PACKAGE format: '$PACKAGE'"
  echo "Expected format: {api,aggregator,online,spark_uber,spark_embedded|service}_{2.11,2.12,2.13} OR flink_2.12"
  exit 1
fi

# Check token
if [[ -z "${SONATYPE_TOKEN:-}" ]]; then
  echo "❌ SONATYPE_TOKEN environment variable is not set"
  echo "Please export your token: export SONATYPE_TOKEN=your_token_here"
  exit 1
fi

# Configuration paths
STAGING_ROOT="target/sonatype-staging"
PACKAGE_PATH="$STAGING_ROOT/$VERSION/ai/chronon/$PACKAGE"
BUNDLE_DIR="$STAGING_ROOT/${VERSION}-bundle"
ZIP_NAME="${PACKAGE}-${VERSION}.zip"
ZIP_PATH="$BUNDLE_DIR/$ZIP_NAME"

# Check package directory
if [[ ! -d "$PACKAGE_PATH" ]]; then
  echo "❌ Package directory does not exist: $PACKAGE_PATH"
  exit 1
fi

# Create output directory
mkdir -p "$BUNDLE_DIR"

# Get absolute path to zip file
ABS_ZIP_PATH="$(realpath "$ZIP_PATH" 2>/dev/null || cd "$BUNDLE_DIR" && pwd)/$ZIP_NAME"

# Create zip from inside version directory
echo "📦 Creating zip for ai/chronon/$PACKAGE → $ABS_ZIP_PATH"
(
  cd "$STAGING_ROOT/$VERSION"
  zip -9 -r "$ABS_ZIP_PATH" "ai/chronon/$PACKAGE"
)

# Upload with retry logic
MAX_RETRIES=1000
RETRY_DELAY=5
attempt=1
success=0

echo "🚀 Uploading $ABS_ZIP_PATH to Sonatype Central..."

while [[ $attempt -le $MAX_RETRIES ]]; do
  echo "📡 Attempt $attempt of $MAX_RETRIES..."
  if curl --request POST \
    --header "Authorization: Bearer $SONATYPE_TOKEN" \
    --form "bundle=@$ABS_ZIP_PATH" \
    --silent --show-error --fail \
    https://central.sonatype.com/api/v1/publisher/upload; then
    echo ""
    echo "✅ Upload succeeded for package '$PACKAGE' on attempt $attempt"
    success=1
    break
  else
    echo "⚠️ Upload failed for package '$PACKAGE' on attempt $attempt. Retrying in ${RETRY_DELAY}s..."
    sleep $RETRY_DELAY
    attempt=$((attempt + 1))
  fi
done

# Always delete the zip file
echo "🧹 Cleaning up local zip: $ABS_ZIP_PATH"
rm -f "$ABS_ZIP_PATH"

# Final result
if [[ $success -eq 0 ]]; then
  echo "❌ All $MAX_RETRIES upload attempts failed for package '$PACKAGE'."
  exit 1
else
  exit 0
fi