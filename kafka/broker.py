from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.client_async import KafkaClient

def main():
    # 서버 주소 설정
    zookeeper_servers = ["localhost:2181", "localhost:2182"]
    # 카프카 서버 설정
    bootstrap_servers = ["localhost:9092", "localhost:9093"]

    #브로커 상태 확인
    client = KafkaClient(bootstrap_servers=bootstrap_servers)
    print(client.cluster.brokers())

if __name__ == "__main__":
    main()
