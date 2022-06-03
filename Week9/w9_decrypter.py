#!/usr/bin/env python
from confluent_kafka import Consumer
import ccloud_lib
import json
from datetime import date
import os
import zlib
import rsa
from cryptography.fernet import Fernet
from google.cloud import storage

# Provide path for service account key for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"/home/mina8/.config/gcp_service_key.json"

def download_blob_into_memory(bucket_name, blob_name, fernet_key):
    # Downloads an object from the bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    encrypted_data = blob.download_as_string()

    # Decrypt compressed data
    fernet = Fernet(fernet_key)
    compressed_data = fernet.decrypt(encrypted_data)

    # Decompress contents
    contents = zlib.decompress(compressed_data).decode("utf-8")

    with open("tempfile", "w") as file:
        file.write(contents)
        file.write("\n")

    print(f"Contents downloaded, decrypted, and decompressed in tempfile")

if __name__ == '__main__':
    with open("dataeng_private_key", "r") as secret_file:
        private_key = rsa.PrivateKey.load_pkcs1(secret_file.read())

    with open("dataeng_fernet_key", "rb") as secret_file:
        rsa_encrypt_fernet_key = secret_file.read()
        fernet_key = rsa.decrypt(rsa_encrypt_fernet_key, private_key)
        #print(fernet_key)

    bucket_name = "archivetest_bucket"
    print(f"{bucket_name}")

    download_blob_into_memory(bucket_name, "en_2022-05-28.json.zlib", fernet_key)
