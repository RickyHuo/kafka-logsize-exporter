# kafka-logsize-exporter

## Installation


download, uncompress

## Getting Started

```bash
vim cluster.conf
```

```
# cluster name alias
[kafka1003]
# zookeeper
zk = 127.0.0.1:2128/kafka1003
# kafka broker list
brokers = broker1,broker2
```

```
python logsize_exporter.py
```

## Result

Take data from 127.0.0.1:8082/metrics
