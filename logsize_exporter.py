# encoding: utf-8

import sys
import threading
import ConfigParser

import prometheus_client

from prometheus_client import Counter, Gauge
from prometheus_client.core import CollectorRegistry
from flask import Response, Flask

from kazoo.client import KazooClient
from kafka.consumer import KafkaConsumer


app = Flask(__name__)

REGISTRY = CollectorRegistry(auto_describe=False)

conf = ConfigParser.ConfigParser()
conf.read("cluster.conf")

cluster = conf.sections()[0]
zookeepers = conf.get(cluster, "zk")
kafka_broker = conf.get(cluster, "brokers")
broker_list = kafka_broker.split(",")
zk = KazooClient(hosts=zookeepers, read_only=True)
zk.start()
if not zk.exists("/brokers/topics"):
    sys.exit(0)


kafka_logsize = Gauge("logsize",
                      "Total count of kafka offset logsize",
                      ["topic", "partition"],
                      registry=REGISTRY)


requests_total = Counter("request_count",
                         "Total request count of the hosts",
                         registry=REGISTRY)


def thread_main(topic):
    consumer = KafkaConsumer(topic,
                             group_id='kafka_monitor',
                             metadata_broker_list=broker_list)

    offset = consumer._offsets.fetch

    for part in offset:
        kafka_logsize.labels(topic=part[0], partition=part[1]).set(
            offset[part]
        )


@app.route("/metrics")
def log_size():
    # request_count + 1
    requests_total.inc()

    topics = zk.get_children("/brokers/topics")
    for i in range(len(topics)):
        topic = topics[i]
        t = threading.Thread(target=thread_main, args=(topic, ))
        t.start()

    return Response(prometheus_client.generate_latest(REGISTRY),
                    mimetype="text/plain")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8082, threaded=True)
