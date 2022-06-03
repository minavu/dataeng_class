#!/usr/bin/env python
# File: se-producer.py

from confluent_kafka import Producer, KafkaError
import re
import json
import ccloud_lib
from datetime import date
from urllib.request import urlopen
from bs4 import BeautifulSoup

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
        print("On {}, {} messages were produced to topic {} with key {}!".format(date.today().strftime("%Y-%m-%d"), delivered_records, topic, key))

    def produce_html(html, FILE_NAME):
        soup = BeautifulSoup(html, 'lxml')
        all_h3s = soup.find_all('h3')
        all_ids = []
        for h3 in all_h3s:
            match = re.search(r'\d{9}', h3.string)
            all_ids.append(match.group())
        trip_data = []
        all_tables = soup.find_all('table')
        for i in range(len(all_tables)):
            t_td = all_tables[i].find_all('td')
            data = {}
            data["trip_id"] = all_ids[i]
            data["route_id"] = t_td[3].string
            data["vehicle_id"] = t_td[0].string
            data["service_key"] = t_td[5].string
            data["direction"] = t_td[4].string
            trip_data.append(data)

        with open(FILE_NAME, "w") as file:
            json.dump(trip_data, file)
        with open(FILE_NAME) as file:
            data = json.load(file)
            produce_data(data, FILE_NAME)
        delivered_records = 0

    if infile is None:
        # gather stop events and produce
        FILE_DATE = date.today().strftime("%Y-%m-%d")
        FILE_NAME = f"stop_events/se-{FILE_DATE}.json"
        html = urlopen("http://psudataeng.com:8000/getStopEvents")
        produce_html(html, FILE_NAME)
    else:
        # read html file and produce
        for i in infile:
            FILE_DATE = re.search(r'\d\d\d\d-\d\d-\d\d', i).group()
            FILE_NAME = f"stop_events/se-{FILE_DATE}.json"
            with open(i) as html:
                produce_html(html, FILE_NAME)

