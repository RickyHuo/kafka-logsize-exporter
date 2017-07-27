# encodings: utf-8

from kazoo.client import KazooClient
from kafka.consumer import KafkaConsumer
from flask import Flask
 
app = Flask(__name__)
 
@app.route('/')
def hello_world():
    return 'Hello\n World'
 
zookeepers = "10.77.115.46:2181/kafka/sina1003"
kafka_broker = "10.77.115.92"
zk = KazooClient(hosts=zookeepers, read_only=True)
zk.start()

def output_format(topic, logsize, partition):
    line = 'logsize{{topic="{0}",partition="{2}"}} {1}'.format(
        topic,
        logsize,
        partition
    )
    return line

@app.route("/metrics")
def start():
    html = ""

    if zk.exists("/brokers/topics"):
        topics = zk.get_children("/brokers/topics")
        for i in range(len(topics)):
            topic = topics[i]

            broker_list = kafka_broker.split(",")
            consumer = KafkaConsumer(topic,
                                     group_id='kafka_monitor',
                                     metadata_broker_list=broker_list)

            offset = consumer._offsets.fetch
            consumer.close()

            for part in offset:
                line = output_format(part[0], offset[part], part[1])
                html += line + "\n"

    return html



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082, threaded=True)
