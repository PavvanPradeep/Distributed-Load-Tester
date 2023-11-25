from flask import Flask, request, jsonify
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import random
import json
import threading
import sqlite3

class Orchestrator:
    def __init__(self):
        self.app = Flask(__name__)
        self.admin = self.setup_kafka_topics()
        self.consumer, self.producer = self.setup_kafka()
        self.nodes_list = []

        # SQLite initialization
        self.conn = sqlite3.connect('metrics.db')
        self.cursor = self.conn.cursor()

        # Start a thread for Flask API
        api_thread = threading.Thread(target=self.start_api)
        api_thread.start()

    def setup_kafka_topics(self):
        admin = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='Orchestrator')
        topics = ['test_config', 'heartbeat', 'register', 'metrics', 'trigger']
        new_topics = [NewTopic(name=topic, num_partitions=3, replication_factor=1) for topic in topics]
        admin.create_topics(new_topics)
        return admin

    def exit_code(self):
        topics = ['test_config', 'heartbeat', 'register', 'metrics', 'trigger']
        self.admin.delete_topics(topics=topics)

    def setup_kafka(self):
        print("Setting Up Kafka Infrastructure")
        producer = KafkaProducer(bootstrap_servers="localhost:9092")
        consumer = KafkaConsumer()
        consumer.subscribe(["register", "heartbeat", "metrics"])
        return consumer, producer

    def register_node(self, node_data):
        try:
            node = json.loads(node_data)
            self.nodes_list.append(node)
            print(f'Node registered: {node}')
            return {"message": "Node registered successfully"}, 200
        except json.JSONDecodeError:
            return {"error": "Invalid JSON format for node registration"}, 400

    def start_api(self):
        @self.app.route('/register', methods=['POST'])
        def register():
            return self.register_node(request.data)

        # Run Flask app
        self.app.run(port=5000)

    def make_config(self):
        test_config_mesg = {
            "test_id": str(random.randint(100000, 999999)),
            "test_type": "",
            "test_message_delay": 0.00,
            "throughput_per_driver": 0,
        }
        test_config_mesg["throughput_per_driver"] = int(input("Enter the Desired Throughput Per Driver: "))
        test_type = input("Select Type of Test: (A) Avalanche / (T) Tsunami: ")
        if test_type == "A":
            test_config_mesg["test_type"] = "AVALANCHE"
        elif test_type == "T":
            test_config_mesg["test_type"] = "TSUNAMI"
            test_config_mesg["test_message_delay"] = float(input("Enter delay: "))
        return test_config_mesg

    def make_trigger(self):
        trigger_req = {
            "test_id": str(random.randint(100000, 999999)),
            "trigger": "YES",
        }
        return trigger_req

    def start_test(self):
        print("Prepping test_config")
        test_config_mesg = self.make_config()
        test_config_mesg_str = json.dumps(test_config_mesg)
        try:
            self.producer.send("test_config", test_config_mesg_str.encode("utf-8"))
            start = input("Trigger Test? (y/n): ")
            if start.lower() == 'y':
                print("Triggering Test")
                trigger_req = self.make_trigger()
                trigger_req_str = json.dumps(trigger_req)
                self.producer.send("trigger", trigger_req_str.encode("utf-8"))
        except KafkaError as e:
            print(f"Error sending message to Kafka: {e}")

    def print_metrics(self,metrics):
        for message in self.consumer:
            try:
                msg = message.value.decode('utf8').replace("'", '"')
                data = json.loads(msg)
                if message.topic == "heartbeat":
                    print(f'This is heartbeat from {data["node_id"]}')
                elif message.topic == "metrics":
                    print(f'This is metrics from {data["node_id"]}')
                    print(data["metrics"])
            except json.JSONDecodeError:
                print("Invalid JSON format received from Kafka.")


    def store_metrics_in_database(self, metrics):
        # Insert metrics data into the table
        self.cursor.execute('''
            INSERT INTO metrics (node_id, test_id, mean_latency, median_latency, min_latency, max_latency)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            metrics["node_id"],
            metrics["test_id"],
            metrics["metrics"]["mean_latency"],
            metrics["metrics"]["median_latency"],
            metrics["metrics"]["min_latency"],
            metrics["metrics"]["max_latency"]
        ))

        # Commit changes
        self.conn.commit()

    def query_metrics(self):
        # Execute a sample query (replace this with your specific query)
        self.cursor.execute('SELECT * FROM metrics')

        # Fetch all the rows
        rows = self.cursor.fetchall()

        # Print or process the results
        for row in rows:
            metrics = {
                "node_id": row[1],
                "test_id": row[2],
                "metrics": {
                    "mean_latency": row[3],
                    "median_latency": row[4],
                    "min_latency": row[5],
                    "max_latency": row[6]
                }
            }
            print(metrics)
            self.print_metrics(metrics)

    def close_connection(self):
        # Close the connection
        self.conn.close()

    def run(self):
        while True:
            print("Choose Option -")
            print("[1] : Start Test")
            print("[2] : Print Metrics")
            print("[0] : Exit")
            option = input("")
            if option == "1":
                self.start_test()
            elif option == "2":
                self.query_metrics()
            elif option == "0":
                print("Exiting Application .... ")
                self.close_connection()
                self.sys.exit(1)
                break
            else:
                print("Invalid option. Please try again.")

    def run_driver_interaction(self):
        print("Registering Nodes ... ")
        num_of_nodes = int(input("Enter the number of Drivers between 2 - 8: "))
        print("Start your Driver Nodes ....")
        for _ in range(num_of_nodes):
            node_data = input("Enter JSON data for the node: ")
            self.register_node(node_data)

if __name__ == "__main__":
    orchestrator = Orchestrator()
    print("Starting ... ")
    print("Importing Libraries ... ")
    orchestrator.run_driver_interaction()
    orchestrator.run()