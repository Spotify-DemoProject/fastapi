import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(current_dir, f'../lib')
config_dir = os.path.join(current_dir, f'../config/config.ini')
sys.path.append(lib_dir)

from kafka_libs import *

def publish_message_kafka(insert_date:str, category:str):
    from configparser import ConfigParser
    
    config = ConfigParser()
    config.read(config_dir)
    
    broker = config.get("Kafka", "broker")
    topic = "spotify-raw"
    key = f"{category}-{insert_date}"
    message = str({"insert_date":insert_date, "category":category})
    
    publish_message(broker=broker, topic=topic, key=key, message=message)

if __name__ == "__main__":
    # publish_message_kafka(insert_date="2023-12-29", category="artists")
    # publish_message_kafka(insert_date="2023-12-29", category="albums")
    # publish_message_kafka(insert_date="2023-12-29", category="tracks/main")
    publish_message_kafka(insert_date="2023-12-29", category="tracks/audio_features")