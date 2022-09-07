import time

from kafka import KafkaProducer


def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    i = 0
    while True:
        data = f"Data number {i}"
        producer.send("stream_topic", str.encode(data))
        print(data, "Was send")
        i+=1
        time.sleep(1)



if __name__ == '__main__':
    main()
