import logging
import logging.handlers
import json
import socket
import requests

def main():
    host = '127.0.0.1'
    
    test_logger = logging.getLogger('python-flume-logger')
    test_logger.setLevel(logging.INFO)

#    test_logger.addHandler(logstash.LogstashHandler(host, 5000, version=1))
#   test_logger.addHandler(logstash.TCPLogstashHandler(host, 44444, version=1))
#   test_logger.addHandler(logging.handlers.SocketHandler(host,44444))
    test_logger.addHandler(logging.handlers.SocketHandler(host, 5000))
    extra = {
        "name": "taebin2",
        "age": 17,
        "value": 13
    }
    extra = json.dumps(extra)
    #test_logger.error(extra)
    test_logger.info(extra)
    #test_logger.warning(extra)


def main1():

    HOST = '127.0.0.1'
# 서버에서 지정해 놓은 포트 번호입니다.
    PORT = 5000
# 소켓 객체를 생성합니다.
# 주소 체계(address family)로 IPv4, 소켓 타입으로 TCP 사용합니다.
    a = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# 지정한 HOST와 PORT를 사용하여 서버에 접속합니다.
    a.connect((HOST, PORT))
# 메시지를 전송합니다.
    a.send("sss".encode())
# 소켓을 닫습니다.
    a.close()


def netcat(hostname, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((hostname, port))
    s.send("test words 1\n".encode())
    # s.send("test words 2\n")
    # s.send("test words 3\n")
    # s.send("test words 4\n")
    s.shutdown(socket.SHUT_WR)
    s.close()

if __name__ == "__main__":
    netcat("127.0.0.1", 5000)
