import time
import random
import datetime
from kafka import KafkaProducer

bootstrap_servers = ['localhost:9092','localhost:9093']  # kafka broker ip
topicName = 'peter-topic'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

for i, _ in enumerate(range(5)):

    # test1 - send numeric type
    print(i)
    producer.send(topicName, str(i).encode())

    # test2 = send string type
    text = 'This is ' + str(i) + ' msg'
    print(text)

    tim = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    producer.send(topicName, text.encode())
    producer.send(topicName, tim.encode())

    time.sleep(1)
