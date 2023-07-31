#! encoding=utf-8

import argparse
import logging
import multiprocessing
import time
from multiprocessing import pool

import kafka_example_common as common
import kafka_example_consumer as consumer
import kafka_example_producer as producer

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-kafka-broker', type=str, default='localhost:9092',
                        help='kafka borker host. default is `localhost:9200`')
    parser.add_argument('-kafka-topic', type=str, default='tdengine-kafka-practices',
                        help='kafka topic. default is `tdengine-kafka-practices`')
    parser.add_argument('-kafka-group', type=str, default='kafka_practices',
                        help='kafka consumer group. default is `kafka_practices`')
    parser.add_argument('-taos-host', type=str, default='localhost',
                        help='TDengine host. default is `localhost`')
    parser.add_argument('-taos-port', type=int, default=6030, help='TDengine port. default is 6030')
    parser.add_argument('-taos-user', type=str, default='root', help='TDengine username, default is `root`')
    parser.add_argument('-taos-password', type=str, default='taosdata', help='TDengine password, default is `taosdata`')
    parser.add_argument('-taos-db', type=str, default='tdengine_kafka_practices',
                        help='TDengine db name, default is `tdengine_kafka_practices`')
    parser.add_argument('-table-count', type=int, default=100, help='TDengine sub-table count, default is 100')
    parser.add_argument('-table-items', type=int, default=1000, help='items in per sub-tables, default is 1000')
    parser.add_argument('-message-type', type=str, default='line',
                        help='kafka message type. `line` or `json`. default is `line`')
    parser.add_argument('-max-poll', type=int, default=1000, help='max poll for kafka consumer')
    parser.add_argument('-threads', type=int, default=10, help='thread count for deal message')
    parser.add_argument('-processes', type=int, default=1, help='process count')

    args = parser.parse_args()
    total = args.table_count * args.table_items

    logging.warning("## start to prepare testing data...")
    prepare_data_start = time.time()
    producer.produce_total(100, args.kafka_broker, args.kafka_topic, args.message_type, total, args.table_count)
    prepare_data_end = time.time()
    logging.warning("## prepare testing data finished! spend-[%s]", prepare_data_end - prepare_data_start)

    logging.warning("## start to create database and tables ...")
    create_db_start = time.time()
    # create database and table
    common.create_database_and_tables(host=args.taos_host, port=args.taos_port, user=args.taos_user,
                                      password=args.taos_password, db=args.taos_db, table_count=args.table_count)
    create_db_end = time.time()
    logging.warning("## create database and tables finished! spend [%s]", create_db_end - create_db_start)

    processes = args.processes

    logging.warning("## start to consume data and insert into TDengine...")
    consume_start = time.time()
    if processes > 1:  # multiprocess
        multiprocessing.set_start_method("spawn")
        pool = pool.Pool(processes)

        consume_start = time.time()
        for _ in range(processes):
            pool.apply_async(func=consumer.consume, args=(
                args.kafka_broker, args.kafka_topic, args.kafka_group, args.taos_host, args.taos_port, args.taos_user,
                args.taos_password, args.taos_db, args.message_type, args.max_poll, args.threads))
        pool.close()
        pool.join()
    else:
        consume_start = time.time()
        consumer.consume(kafka_brokers=args.kafka_broker, kafka_topic=args.kafka_topic, kafka_group_id=args.kafka_group,
                         taos_host=args.taos_host, taos_port=args.taos_port, taos_user=args.taos_user,
                         taos_password=args.taos_password, taos_database=args.taos_db, message_type=args.message_type,
                         max_poll=args.max_poll, workers=args.threads)
    consume_end = time.time()
    logging.warning("## consume data and insert into TDengine over! spend-[%s]", consume_end - consume_start)

    # print report
    logging.warning(
        "\n#######################\n"
        "     Prepare data      \n"
        "#######################\n"
        "# data_type # %s  \n"
        "# total     # %s  \n"
        "# spend     # %s s\n"
        "#######################\n"
        "     Create database   \n"
        "#######################\n"
        "# stable    # 1  \n"
        "# sub-table # 100  \n"
        "# spend     # %s s \n"
        "#######################\n"
        "        Consume        \n"
        "#######################\n"
        "# data_type   # %s  \n"
        "# threads     # %s  \n"
        "# processes   # %s  \n"
        "# total_count # %s  \n"
        "# spend       # %s s\n"
        "# per_second  # %s  \n"
        "#######################\n",
        args.message_type, total, prepare_data_end - prepare_data_start, create_db_end - create_db_start,
        args.message_type, args.threads, processes, total, consume_end - consume_start,
                                  total / (consume_end - consume_start))
