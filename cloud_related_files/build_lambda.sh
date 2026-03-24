#!/bin/bash
# build_lambda.sh
# ================
# Packages the Lambda function + azure-storage-blob dependency into a zip
# ready for `terraform apply` or manual upload to AWS console.
#
# Run from the project root:
#   chmod +x build_lambda.sh && ./build_lambda.sh

set -euo pipefail

BUILD_DIR="lambda_build"
ZIP_NAME="lambda_package.zip"

echo "► Cleaning previous build…"
rm -rf "$BUILD_DIR" "$ZIP_NAME"
mkdir "$BUILD_DIR"

echo "► Installing azure-storage-blob into build dir…"
pip install azure-storage-blob --target "$BUILD_DIR" --quiet

echo "► Copying Lambda handler…"
cp s3_to_adls_lambda.py "$BUILD_DIR/"

echo "► Zipping package…"
cd "$BUILD_DIR"
zip -r "../$ZIP_NAME" . -x "*.pyc" -x "*/__pycache__/*" > /dev/null
cd ..

echo "✓ Built $ZIP_NAME ($(du -sh $ZIP_NAME | cut -f1))"
echo ""
echo "Next: terraform init && terraform apply -var=\"azure_sas_token=\$AZURE_SAS_TOKEN\""
