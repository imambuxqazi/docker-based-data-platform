#!/bin/sh

set -eu

echo "Starting MinIO bucket initialization..."

MINIO_ALIAS_NAME="localminio"

create_bucket_if_missing() {
  bucket_name="$1"

  if mc ls "${MINIO_ALIAS_NAME}/${bucket_name}" >/dev/null 2>&1; then
    echo "Bucket already exists: ${bucket_name}"
  else
    mc mb "${MINIO_ALIAS_NAME}/${bucket_name}"
    echo "Created bucket: ${bucket_name}"
  fi
}

create_placeholder_object() {
  bucket_name="$1"
  object_path="$2"
  temp_file="/tmp/placeholder.txt"

  printf "initialized at %s\n" "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" > "${temp_file}"
  mc cp "${temp_file}" "${MINIO_ALIAS_NAME}/${bucket_name}/${object_path}" >/dev/null
  echo "Created placeholder object: s3://${bucket_name}/${object_path}"
}

set_public_policy() {
  bucket_name="$1"
  mc anonymous set download "${MINIO_ALIAS_NAME}/${bucket_name}"
  echo "Applied public read policy to bucket: ${bucket_name}"
}

create_bucket_if_missing "${BRONZE_BUCKET}"
create_bucket_if_missing "${SILVER_BUCKET}"
create_bucket_if_missing "${GOLD_BUCKET}"

create_placeholder_object "${BRONZE_BUCKET}" "orders/_PLACEHOLDER"
create_placeholder_object "${BRONZE_BUCKET}" "clickstream/_PLACEHOLDER"
create_placeholder_object "${BRONZE_BUCKET}" "checkpoints/_PLACEHOLDER"

create_placeholder_object "${SILVER_BUCKET}" "orders/_PLACEHOLDER"
create_placeholder_object "${SILVER_BUCKET}" "clickstream/_PLACEHOLDER"
create_placeholder_object "${SILVER_BUCKET}" "quality/_PLACEHOLDER"

create_placeholder_object "${GOLD_BUCKET}" "sales/_PLACEHOLDER"
create_placeholder_object "${GOLD_BUCKET}" "funnel/_PLACEHOLDER"
create_placeholder_object "${GOLD_BUCKET}" "segments/_PLACEHOLDER"
create_placeholder_object "${GOLD_BUCKET}" "traffic/_PLACEHOLDER"
create_placeholder_object "${GOLD_BUCKET}" "alerts/_PLACEHOLDER"

set_public_policy "${BRONZE_BUCKET}"
set_public_policy "${SILVER_BUCKET}"
set_public_policy "${GOLD_BUCKET}"

echo "Listing bucket contents for verification..."
mc ls "${MINIO_ALIAS_NAME}"
echo "MinIO bucket initialization completed successfully."