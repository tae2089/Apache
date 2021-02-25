from confluent_kafka import Consumer, KafkaError

def main():
    settings = {
        'bootstrap.servers': 'localhost:9092,localhost:9093',
        'group.id': 'mygroup',
        'client.id': 'client-1',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }

    c = Consumer(settings)
    c.subscribe(['peter-topic'])
    try:
        while True:
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                a = msg.value().decode("utf-8")
                # binary String 을 String 으로 change 필요
                print('Received message: {0}'.format(a))
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
            else:
                print('Error occured: {0}'.format(msg.error().str()))
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    main()
