from taos.cinterface import *
from taos.error import *

TMQ_RES_INVALID = -1
TMQ_RES_DATA = 1
TMQ_RES_TABLE_META = 2
TMQ_RES_METADATA = 3


class MessageBlock:
    def __init__(self, block=None, fields=None, row_count=0, col_count=0, table=""):
        # type (list[tuple], TaosField, int, int, str)
        self._block = block
        self._fields = fields
        self._rows = row_count
        self._cols = col_count
        self._table = table

    def fields(self):
        # type: () -> TaosField
        """
        Get fields in message block
        """
        return self._fields

    def nrows(self):
        # type: () -> int
        """
        get total count of rows of message block
        """
        return self._rows

    def ncols(self):
        # type: () -> int
        """
        get total count of rows of message block
        """
        return self._cols

    def fetchall(self):
        # type: () -> list[tuple]
        """
        get all data in message block
        """
        return list(map(tuple, zip(*self._block)))

    def table(self):
        # type: () -> str
        """
        get table name of message block
        """
        return self._table

    def __iter__(self):
        return iter(self.fetchall())


class Message:
    def __init__(self, msg: c_void_p = None, error=None, decode_binary=True):
        self._error = error
        if not msg:
            return
        self.msg = msg
        err_no = taos_errno(self.msg)
        if err_no:
            self._error = TmqError(msg=taos_errstr(self.msg))
        self.decode_binary = decode_binary

    def error(self):
        # type: () -> TmqError | None
        """

        The message object is also used to propagate errors and events, an application must check error() to determine
        if the Message is a proper message (error() returns None) or an error or event (error() returns a TmqError
         object)

          :rtype: None or :py:class:`TmqError
        """
        return self._error

    def topic(self):
        # type: () -> str
        """

        :returns: topic name.
          :rtype: str
        """
        return tmq_get_topic_name(self.msg)

    def database(self):
        # type () -> str
        """

        :returns: database name.
          :rtype: str
        """
        return tmq_get_db_name(self.msg)

    def value(self):
        # type: () -> list[MessageBlock] | None
        """
        :returns: message value (payload).
          :rtype: list[tuple]
        """

        res_type = tmq_get_res_type(self.msg)
        if res_type == TMQ_RES_TABLE_META or res_type == TMQ_RES_INVALID:
            return None

        message_blocks = []
        while True:
            block, num_rows = taos_fetch_block_raw(self.msg)
            if num_rows == 0:
                break
            field_count = taos_num_fields(self.msg)
            fields = taos_fetch_fields(self.msg)
            precision = taos_result_precision(self.msg)

            blocks = [None] * field_count
            for i in range(len(fields)):
                if fields[i]["type"] not in CONVERT_FUNC_BLOCK_v3 and fields[i]["type"] not in CONVERT_FUNC_BLOCK:
                    raise TmqError("Invalid data type returned from database")

                block_data = ctypes.cast(block, ctypes.POINTER(ctypes.c_void_p))[i]
                if fields[i]["type"] in (
                    FieldType.C_VARCHAR,
                    FieldType.C_NCHAR,
                    FieldType.C_JSON,
                    FieldType.C_VARBINARY,
                    FieldType.C_GEOMETRY,
                    FieldType.C_BLOB,
                ):
                    f = convert_block_func_v3(fields[i]["type"], self.decode_binary)
                    offsets = taos_get_column_data_offset(self.msg, i, num_rows)
                    blocks[i] = f(block_data, [], num_rows, offsets, precision)
                else:
                    f = convert_block_func(fields[i]["type"], self.decode_binary)
                    is_null = taos_is_null_by_column(self.msg, num_rows, i)
                    blocks[i] = f(block_data, is_null, num_rows, [], precision)

            message_blocks.append(
                MessageBlock(
                    block=blocks,
                    fields=fields,
                    row_count=num_rows,
                    col_count=field_count,
                    table=tmq_get_table_name(self.msg),
                )
            )
        return message_blocks

    def offset(self):
        # type: () -> int
        """
        :returns: message offset.
          :rtype: int
        """
        return tmq_get_vgroup_offset(self.msg)

    def __del__(self):
        if not self.msg:
            return
        taos_free_result(self.msg)

    def __iter__(self):
        return iter(self.value())


class TopicPartition:
    def __init__(self, topic, partition, offset, begin=0, end=0):
        self.topic = topic  # type: str
        self.partition = partition  # type: int
        self.offset = offset  # type: int
        self.begin = begin  # type: int
        self.end = end  # type: int

    def __str__(self):
        return "TopicPartition(topic=%s, partition=%s, offset=%s)" % (self.topic, self.partition, self.offset)


class Consumer:
    def __init__(self, configs):
        if "group.id" not in configs:
            raise TmqError("missing group.id in consumer config setting")

        self._tmq = None
        self._subscribed = False
        self.decode_binary = True
        tmq_conf = tmq_conf_new()
        try:
            for key in configs:
                if key == "decode_binary":
                    self.decode_binary = configs[key]
                    continue
                value = configs[key]
                if key == "td.connect.bearer_token":
                    key = "td.connect.token"
                tmq_conf_set(tmq_conf, key=key, value=value)

            self._tmq = tmq_consumer_new(tmq_conf)
        finally:
            tmq_conf_destroy(tmq_conf)

    def subscribe(self, topics):
        # type ([str]) -> None
        """
        Set subscription to supplied list of topics.
        :param list(str) topics: List of topics (strings) to subscribe to.
        """
        if not topics or len(topics) == 0:
            raise TmqError("Unset topic for Consumer")

        class TmqListInner:
            def __init__(self, topics) -> None:
                self.ptr = tmq_list_new()
                for topic in topics:
                    self.append(topic)

            def append(self, item: str):
                res = tmq_list_append(self.ptr, item)
                if res != 0:
                    raise TmqError(msg="fail on parse topics", errno=res)

            def __del__(self):
                tmq_list_destroy(self.ptr)

        topic_list = TmqListInner(topics)
        tmq_subscribe(self._tmq, topic_list.ptr)
        del topic_list
        self._subscribed = True

    def unsubscribe(self):
        """
        Remove current subscription.
        """
        tmq_unsubscribe(self._tmq)
        self._subscribed = False

    def poll(self, timeout: float = 1.0):
        # type (float) -> Message | None
        """
        Consumes a single message and returns events.

        The application must check the returned `Message` object's `Message.error()` method to distinguish between
        proper messages (error() returns None).

        :param float timeout: Maximum time to block waiting for message, event or callback (default: 1). (second)
        :returns: A Message object or None on timeout
        :rtype: `Message` or None
        """
        mill_timeout = int(timeout * 1000)
        if not self._subscribed:
            raise TmqError(msg="unsubscribe topic")

        msg = tmq_consumer_poll(self._tmq, wait_time=mill_timeout)
        if msg:
            return Message(msg=msg, decode_binary=self.decode_binary)
        return None

    def assignment(self):
        """
        Returns the current partition assignment as a list of TopicPartition tuples.
        """
        topics = tmq_subscription(self._tmq)
        if not topics:
            return None

        topic_partitions = []
        for topic in topics:
            assignments = tmq_get_topic_assignment(self._tmq, topic)
            for assignment in assignments:
                topic_partitions.append(
                    TopicPartition(
                        topic=topic,
                        partition=assignment[0],
                        offset=assignment[1],
                        begin=assignment[2],
                        end=assignment[3],
                    )
                )
        return topic_partitions

    def seek(self, partition):
        # type (TopicPartition) -> None
        """
        Set consume position for partition to offset.
        """
        tmq_offset_seek(self._tmq, partition.topic, partition.partition, partition.offset)

    def close(self):
        """
        Close down and terminate the Kafka Consumer.
        """
        if self._tmq:
            tmq_consumer_close(self._tmq)
            self._tmq = None

    def commit(self, message: Message = None, offsets: [TopicPartition] = None):
        # type (Message, [TopicPartition], bool) -> None
        """
        Commit a message.

        The `message` and `offsets` parameters are mutually exclusive. If neither is set, the current partition
        assignment's offsets are used instead. Use this method to commit offsets if you have 'enable.auto.commit' set
        to False.

        :param Message message: Commit the message's offset+1. Note: By convention, committed offsets reflect the next
            message to be consumed, **not** the last message consumed.
        :param list(TopicPartition) offsets: List of topic+partitions+offsets to commit.
        """
        if message:
            if not isinstance(message, Message):
                raise TmqError(msg="Invalid message type")
            tmq_commit_sync(self._tmq, message.msg)
            return

        if offsets and isinstance(offsets, list):
            for offset in offsets:
                if not isinstance(offset, TopicPartition):
                    raise TmqError(msg="Invalid offset type")
                tmq_commit_offset_sync(self._tmq, offset.topic, offset.partition, offset.offset)
            return

        tmq_commit_sync(self._tmq, None)

    def committed(self, partitions):
        # type ([TopicPartition]) -> [TopicPartition]
        """
        Retrieve committed offsets for the specified partitions.

        :param list(TopicPartition) partitions: List of topic+partitions to query for stored offsets.
        :returns: List of topic+partitions with offset and possibly error set.
        :rtype: list(TopicPartition)
        """
        for partition in partitions:
            if not isinstance(partition, TopicPartition):
                raise TmqError(msg="Invalid partition type")
            offset = tmq_committed(self._tmq, partition.topic, partition.partition)
            partition.offset = offset

        return partitions

    def position(self, partitions):
        # type ([TopicPartition]) -> [TopicPartition]
        """
        Retrieve current positions (offsets) for the specified partitions.

        :param list(TopicPartition) partitions: List of topic+partitions to return current offsets for.
        :returns: List of topic+partitions with offset and possibly error set.
        :rtype: list(TopicPartition)
        """
        for partition in partitions:
            if not isinstance(partition, TopicPartition):
                raise TmqError(msg="Invalid partition type")
            offset = tmq_position(self._tmq, partition.topic, partition.partition)
            partition.offset = offset

        return partitions

    def list_topics(self) -> [str]:
        # type () -> [str]
        """
        Request subscription topics from the tmq.

        :rtype: topics list
        """
        return tmq_subscription(self._tmq)

    def __del__(self):
        self.close()

    def __next__(self):
        if not self._tmq:
            raise StopIteration("Tmq consumer is closed")
        return next(self._sync_next())

    def __iter__(self):
        return self

    def _sync_next(self):
        while True:
            message = self.poll()
            if message:
                break
        yield message
