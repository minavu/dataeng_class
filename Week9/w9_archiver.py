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

def create_bucket_class_location(bucket_name):
    # Create new bucket in the US region with coldline storage class
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print("Created bucket {} in {} with storage class {}".format(
                new_bucket.name, new_bucket.location, new_bucket.storage_class
         )
    )
    return new_bucket

def delete_bucket(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.lookup_bucket(bucket_name)

    if bucket is None:
        return

    # Delete all blobs in bucket
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        print(f"Deleting blob {blob.name} in bucket {bucket.name}")
        blob.delete()

    # Delete bucket. The bucket must be empty.
    bucket.delete()
    print(f"Bucket {bucket.name} deleted.")

def upload_blob_from_memory(bucket_name, contents, destination_blob_name, fernet_key):
    # Compress contents before uploading
    compressed_data = zlib.compress(contents.encode('utf-8'))

    # Encrypt compressed data before uploading
    fernet = Fernet(fernet_key)
    encrypted_data = fernet.encrypt(compressed_data)

    # Uploads an object to the bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(encrypted_data)
    print(f"Blob {destination_blob_name} uploaded to {bucket_name}")

if __name__ == '__main__':
    FILE_DATE = date.today().strftime("%Y-%m-%d")

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    setup = args.setup
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_2'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Generate public and private keys for data encryption
    public_key, private_key = rsa.newkeys(2048)
    with open("dataeng_private_key", "wb") as secret_file:
        secret_file.write(private_key.save_pkcs1('PEM'))

    fernet_key = Fernet.generate_key()
    rsa_encrypt_fernet_key = rsa.encrypt(fernet_key, public_key)
    with open("dataeng_fernet_key", "wb") as secret_file:
        secret_file.write(rsa_encrypt_fernet_key)

    # Process messages
    data_array = []
    data_key = ""
    count = 0

    try:
        while True:
            # Check for Kafka message
            msg = consumer.poll(1.0)
            if msg is None:
                if count > 0:
                    bucket_name = "archivetest_bucket"
                    contents = json.dumps(data_array)
                    if setup is True:
                        delete_bucket(bucket_name)
                        gcp_bucket = create_bucket_class_location(bucket_name)
                    upload_blob_from_memory(bucket_name, contents, f"en_{data_key}.zlib", fernet_key)
                    count = 0
                print(f"Archiver: Waiting for message or event/error in poll() from {topic}")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                record_key = msg.key().decode("utf-8")
                record_value = msg.value()
                data = json.loads(record_value)
                data_array.append(data)
                count += 1
                data_key = record_key
                print(f"Archiver: Consumed message with key {record_key} from topic {topic} total count is {count}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close() # Leave group and commit final offsets
