import sys
import argparse

from six.moves import input

from confluent_kafka import avro as avro

record_schema_str = """
    {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number",  "type": ["int", "null"]},
            {"name": "favorite_color", "type": ["string", "null"]}
        ]
    }
"""


class ParserOpts(object):
    def _build(self):
        conf = {'bootstrap.servers': self.bootstrap_servers,
                'schema.registry.url': self.schema_registry}

        if self.userinfo:
            conf['schema.registry.basic.auth.credentials.source'] = 'USER_INFO'
            conf['schema.registry.basic.auth.user.info'] = self.userinfo
        return conf

    def producer_conf(self):
        return self._build()

    def consumer_conf(self):
        return dict({"group.id": self.group}, **self._build())


class User(dict):
    _schema = avro.loads(record_schema_str)

    def __init__(self):
        super(dict, self).__init__({"name": "anonymous",
                                    "favorite_number": 0,
                                    "favorite_color": ""})

    @classmethod
    def schema(cls):
        return cls._schema

    def prompt(self):
        self['name'] = input("Enter name:")
        try:
            num = input("Enter favorite number:")
            if num == '':
                num = 0
            self['favorite_number'] = int(num)
        except ValueError:
            print("invalid integer type, discarding record...")
            self.prompt()

        self['favorite_color'] = input("Enter favorite color:")


def on_delivery(err, msg):
    if err is not None and msg is not None:
        print('Message delivery failed ({} [{}]): %{}'.format(msg.topic(), str(msg.partition()), err))
        return 0
    elif err is not None:
        print('Message delivery failed {}'.format(err))
        return 0
    else:
        print('Message delivered to %s [%s] at offset [%s]: %s' %
              (msg.topic(), msg.partition(), msg.offset(), msg.value()))
        return 1


def produce(args):
    from confluent_kafka.avro import AvroProducer

    record = User()
    topic = args.topic
    conf = args.producer_conf()

    producer = AvroProducer(conf, default_value_schema=record.schema())

    print("adding user records to topic {}. ^c to exit.".format(topic))

    while True:
        try:
            record.prompt()
            producer.produce(topic=topic, partition=0, value=record, callback=on_delivery)
        except KeyboardInterrupt:
            break

    print("\nFlushing records...")
    producer.flush()


def consume(args):
    from confluent_kafka.avro import AvroConsumer
    from confluent_kafka import KafkaError, TopicPartition
    from confluent_kafka.avro.serializer import SerializerError

    topic = args.topic
    conf = args.consumer_conf()
    print("consuming user records from topic {}".format(topic))

    c = AvroConsumer(conf)
    c.assign([TopicPartition(topic, partition=0, offset=0)])

    while True:
        try:
            msg = c.poll(1)

        except SerializerError as e:
            print("Message deserialization failed {}".format(e))
            break

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            else:
                print(msg.error())
                break

        print(msg.value())
    c.close()


def main():
    # To use the cluster execute <source root>/tests/docker/bin/cluster_up.sh.
    # Defaults assume the use of the provided test cluster.
    parser = argparse.ArgumentParser(description="Example client for handling Avro data")
    parser.add_argument('-b', metavar="<brokers,...>", dest="bootstrap_servers",
                        default="localhost:29092", help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', metavar="<schema registry>", dest="schema_registry",
                        default="http://localhost:8083", help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', metavar="[<topic>]", dest="topic", default="example_avro",
                        help="topic name")
    parser.add_argument('-u', metavar="[<userinfo>]", dest="userinfo", default="ckp_tester:test_secret",
                        help="userinfo (username:password); requires Schema Registry with HTTP basic auth enabled")
    parser.add_argument('mode', metavar="mode", choices=['produce', 'consume'],
                        help="Execution mode (produce | consume)")
    parser.add_argument('-g', metavar="[<consumer group>]", dest="group", default="example_avro",
                        help="consumer group; required if running 'consumer' mode")
    conf = ParserOpts()
    parser.parse_args(namespace=conf)

    getattr(sys.modules[__name__], conf.mode)(conf)


if __name__ == '__main__':
    main()
