
def delivery_report(err, msg):
    if err is not None:
        print('Message Deliver Failed - {}'.format(err))
    else:
        print('Message Delivered - {} [{}]'.format(msg.topic(), msg.partition()))

def publish_message(broker:str, topic:str, key:str, message:str, partition:int=0):
    from confluent_kafka import Producer
    
    conf = {'bootstrap.servers': broker}
    producer = Producer(conf)
    producer.produce(topic, 
                     key=key,
                     partition=partition,
                     value=message, 
                     callback=delivery_report)
    producer.flush()
    
def consume_messages(broker:str, group_id:str, topic:str, key:str, partition:int=0):
    from confluent_kafka import Consumer, KafkaError
    
    conf = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            if msg.partition() == partition and msg.key() == key:
                print('Received message: {}'.format(msg.value().decode('utf-8')))

    except KeyboardInterrupt:
        pass
    
    finally:
        consumer.close()
