package com.taosdata.iot.clients;

import com.taosdata.iot.clients.topics.Topic;

import java.io.Closeable;

/**
 * A client that consumes data from a TDengine cluster.
 */
public interface Consumer extends Closeable{

    /**
     * Subscribe to a topic. A topic can be either a table name or a SQL statement. During the process of subscribing,
     * the consumer will attempt to connect to the TDengine server using the given connection properties.
     * @param topic
     * @throws Exception
     */
    void subscribe(Topic topic) throws Exception;

    /**
     * Retrieve the topic subscribed by the consumer
     * @return
     */
    Topic getTopic();

    /**
     * Unsubscribe to a topic
     */
    void unsubscribe();

    /**
     * Synchronously polling data from the TDengine server. The thread that calls this method will be blocked until
     * a non-empty result set is fetched or total processing time exceeds the timeout limit.
     * @param timeout timeout for the polling process, unit in milliseconds
     * @return
     */
    ConsumerResultSet poll(long timeout);


    /**
     * Similar to poll(). Polling data with a user specified query interval. When polling data, the consumer will keep querying the database
     * for data until a non-empty result set is obtained. Query interval defines the time interval between two
     * consecutive queries if last query fetched an empty result set.
     * @param timeout
     * @param queryInterval
     * @return
     */
    ConsumerResultSet poll(long timeout, long queryInterval);

    /**
     * Similar to poll(). The method allows user to provide a maximum number of rows a single polling result set
     * can contain.
     * @param timeout
     * @param queryInterval
     * @param maxPollSize
     * @return
     */
    ConsumerResultSet poll(long timeout, long queryInterval, long maxPollSize);
    /**
     * Manually reset the polling start time to the given timestamp.
     * @param startTime start time for next poll, formatted as a Unix Timestamp
     */
    void seekTo(long startTime);

    /**
     * Reset the polling start time back to 0, which means the next poll will try to fetch every record in that
     * corresponding topic.
     */
    void seekToBeginning();

    /**
     * Retrieve the position of last consumed record
     * @return a Unix timestamp which is the primary key for the last consumed record
     */
    long position();

    /**
     * Close the consumer and the TDengine connection.
     */
    void close();
}

