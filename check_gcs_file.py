#!/usr/bin/env python3
"""Check for existence of a GCS object matching a glob-like pattern.

Usage: python3 scripts/check_gcs_file.py <bucket_name> <glob_pattern>
Exits 0 if any match found, exits 1 otherwise.
"""
import sys
import fnmatch
from google.cloud import storage


def main(argv):
    if len(argv) != 3:
        print("Usage: check_gcs_file.py <bucket_name> <glob_pattern>")
        return 2

    bucket_name = argv[1]
    pattern = argv[2]

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # list all blobs and match using fnmatch (supports wildcards)
    blobs = client.list_blobs(bucket)
    for blob in blobs:
        if fnmatch.fnmatch(blob.name, pattern):
            print(f"FOUND: {blob.name}")
            return 0

    print("No matching files found")
    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
