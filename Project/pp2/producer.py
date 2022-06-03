#!/usr/bin/env python
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from time import sleep
from datetime import date
from urllib.request import urlopen

if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    infile = args.infile
    conf = ccloud_lib.read_ccloud_config(config_file)
    
    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    def acked(err, msg):
        global delivered_records
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1

    def produce_data(data, key):
        batch_count = 0
        for d in data:
            record_key = key
            record_value = json.dumps(d)
            producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
            producer.poll(0)
            batch_count += 1
            if batch_count == 100000:
                producer.flush()
                batch_count = 0
        producer.flush()
        print("On {}, {} messages were produced to topic {}!".format(date.today().strftime("%Y-%m-%d"), delivered_records, topic))


    if infile is None:
        # gather data, save a file to raw_data/, and produce
        HTML = urlopen("http://psudataeng.com:8000/getBreadCrumbData")
        FILE_DATE = date.today().strftime("%Y-%m-%d")
        FILE_NAME = f"raw_data/{FILE_DATE}.json"
        with open(FILE_NAME, "wb") as file:
            file.write(HTML.read())
        with open(FILE_NAME) as file:
            data = json.load(file)
            produce_data(data, FILE_NAME)
        delivered_records = 0
    else:
        for i in infile:
            FILE_NAME = i
            file = open(FILE_NAME)
            data = json.load(file)
            produce_data(data, FILE_NAME)
            delivered_records = 0

