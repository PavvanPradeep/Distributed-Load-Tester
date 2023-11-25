import requests
import json
import time
import random
import sys
import statistics as st
from kafka import KafkaConsumer
from kafka import KafkaProducer

kafka_address = "localhost:9092"
web_address = "http://localhost:8080"

unique_id = random.randint(10000,999999)

def kafka_initialization():
    print("Initializing Kafka...")
    kafka_sender = KafkaProducer(bootstrap_servers="localhost:9092")
    kafka_receiver = KafkaConsumer()
    kafka_receiver.subscribe(["test_config","trigger"])
    return kafka_receiver, kafka_sender

def NodeRegistration():
    print("Node Registration in progress...")
    print(f'Unique Node ID is : {unique_id}')
    registration_info = {
        "node_id": "",
        "node_IP": "",
        "message_type": "DRIVER_NODE_REGISTER"
    }
    registration_info["node_id"] = str(unique_id)
    registration_info["node_IP"] = ""
    registration_info = str(registration_info)
    return registration_info

def calculate_metrics(raw_data,trigger_info,test_config_info):
    metrics_info = {
        "node_id": "<RANDOMLY GENERATED UNIQUE TEST ID>",
        "test_id": "<TEST ID>",
        "report_id": "<RANDOMLY GENERATED ID FOR EACH METRICS MESSAGE>",
        "metrics": {
            "mean_latency": "",
            "median_latency": "",
            "min_latency": "",
            "max_latency": "",
            "requests_per_second": "",
            "average_throughput": "",
            "peak_response_time": "",
        }
    }
    metrics_info["report_id"] = str(random.randint(10000,999999))
    metrics_info["node_id"] = str(unique_id)
    metrics_info["test_id"] = str(trigger_info["test_id"])

    total_requests = len(raw_data)
    total_time = sum(raw_data)

    metrics_info["metrics"]["requests_per_second"] = str(total_requests / total_time)
    metrics_info["metrics"]["average_throughput"] = str(total_requests / total_time)
    metrics_info["metrics"]["peak_response_time"] = str(max(raw_data))
    metrics_info["metrics"]["mean_latency"] = str(st.mean(raw_data))
    metrics_info["metrics"]["median_latency"] = str(st.median(raw_data))
    metrics_info["metrics"]["min_latency"] = str(min(raw_data))
    metrics_info["metrics"]["max_latency"] = str(max(raw_data))
    return metrics_info

def save_metrics_to_db(metrics):
    ''' Save metrics to SQL database '''
    pass

if __name__ == "__main__":
    print("Starting ... ")
    print("Importing Libraries ... ")
    kafka_receiver, kafka_sender = kafka_initialization()
    registration_info = NodeRegistration()
    registration_info_str = json.dumps(registration_info)
    kafka_sender.send("register", registration_info_str.encode("utf-8"))
    print("Ready for Test ..")
    for message in kafka_receiver:
        msg = message.value.decode('utf8').replace("'", '"')
        if message.topic == "test_config":
            print("Received test config")
            print(msg)
            test_config = json.loads(msg)
            for message in kafka_receiver:
                msg = message.value.decode('utf8').replace("'", '"')
                if message.topic == "trigger":
                    print("Test triggered")
                    trigger = json.loads(msg)
                    response_times = []
                    for i in range(int(test_config["throughput_per_driver"])):
                        time.sleep(float(test_config["test_message_delay"]))
                        response = requests.get(web_address)
                        response_times.append(round(response.elapsed.total_seconds()*1000,3))
                    metrics_info = calculate_metrics(response_times,trigger,test_config)
                    save_metrics_to_db(metrics_info)
                    metrics_info_str = json.dumps(metrics_info)
                    kafka_sender.send("metrics",metrics_info_str.encode("utf-8"))
                    print("Test Completed")
