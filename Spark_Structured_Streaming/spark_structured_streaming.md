# Intro


# How to run examples

* Start containers with `docker-compose up -d`
* Paste the following configuration data into a file at `getting_started.ini`
```
[default]
bootstrap.servers=localhost:9092

[consumer]
group.id=python_example_group_1

# 'auto.offset.reset=earliest' to start reading from the beginning of
# the topic if no committed offsets exist.
auto.offset.reset=earliest
```
* Create a topic
``` bash
docker compose exec broker \
  kafka-topics --create \
    --topic purchases \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
```
* Run a producer example:
* Install poetry
* Run `poetry shell`
* Run com command line `chmod u+x confluent_producer_example.py` and `./confluent_producer_example.py getting_started.ini`

**NOTE:**If you have mac m1 i recommend use confluent cloud

## Reference

1. https://developer.confluent.io/get-started/python/#produce-events

## How to run producer

3. Run 