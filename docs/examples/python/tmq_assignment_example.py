import taos
from taos.tmq import Consumer
import taosws


def prepare():
    conn = taos.connect()
    conn.execute("drop topic if exists tmq_assignment_demo_topic")
    conn.execute("drop database if exists tmq_assignment_demo_db")
    conn.execute("create database if not exists tmq_assignment_demo_db wal_retention_period 3600")
    conn.select_db("tmq_assignment_demo_db")
    conn.execute(
        "create table if not exists tmq_assignment_demo_table (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
    conn.execute(
        "create topic if not exists tmq_assignment_demo_topic as select ts, c1, c2, c3 from tmq_assignment_demo_table")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now-2s, 1, 1.0, 'tmq test')")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now-1s, 2, 2.0, 'tmq test')")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now, 3, 3.0, 'tmq test')")


def taos_get_assignment_and_seek_demo():
    prepare()
    consumer = Consumer(
        {
            "group.id": "0",
            # should disable snapshot,
            # otherwise it will cause invalid params error
            "experimental.snapshot.enable": "false",
        }
    )
    consumer.subscribe(["tmq_assignment_demo_topic"])

    # get topic assignment
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment)

    # poll
    consumer.poll(1)
    consumer.poll(1)

    # get topic assignment again
    after_pool_assignments = consumer.assignment()
    for assignment in after_pool_assignments:
        print(assignment)

    # seek to the beginning
    for assignment in assignments:
        consumer.seek(assignment)

    # now the assignment should be the same as before poll
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment)


if __name__ == '__main__':
    taos_get_assignment_and_seek_demo()
