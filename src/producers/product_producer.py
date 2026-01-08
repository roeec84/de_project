import os
import csv
from kafka import KafkaProducer
import time
import random
import json

class ProductProducer():
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        try:
            self.producer = KafkaProducer(
                bootstrap_servers = bootstrap_servers,
                key_serializer=lambda k: k.encode('utf-8'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries = 5
            )
        except Exception as e:
            print(f"Error creating Kafka Producer: {e}")
            raise
        
    def load_catalog(self, file_path):
        if not os.path.exists(file_path):
            print(f"No file path was found: {file_path}")
            return []
        
        with open(file_path, 'r') as f:
            return list(csv.DictReader(f))
        
    def run(self, file_path):
        catalog = self.load_catalog(file_path=file_path)
        if not catalog:
            return
        
        try:
            while True:
                product = random.choice(catalog)
                event_type = random.choices(['view', 'purchase'], weights=[0.8, 0.2])
                payload = {
                    "product_id": product['id'],
                    "product_name": product['name'],
                    "product_price": float(product['price']),
                    "shop_id": product['shop_id'],
                    "event_type": event_type,
                    "event_ts": time.time()
                }

                self.producer.send(topic=self.topic, key=product['id'], value=payload)
                print(f"Sending {product['name']} to {self.topic}")
                time.sleep(random.uniform(0.5, 2.0))
        except KeyboardInterrupt:
            print("Stopping run...")
        finally:
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_FILE = os.path.join(BASE_DIR, "../../data/products.csv")

    producer = ProductProducer(
        bootstrap_servers=['localhost:9092'],
        topic='ecommerce_events'
    )
    
    producer.run(DATA_FILE)