package com.taosdata.iot.clients;

import com.taosdata.iot.clients.exceptions.ConsumerConfigException;
import com.taosdata.iot.clients.exceptions.TaosRuntimeException;
import com.taosdata.iot.clients.topics.STableTopic;
import com.taosdata.iot.clients.topics.TableTopic;
import com.taosdata.iot.clients.topics.Topic;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBResultSet;
import com.taosdata.jdbc.TSDBResultSetMetaData;
import com.taosdata.jdbc.utils.SqlSyntaxValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * A client that consumes data from a TDengine cluster.
 * <p>The TaosConsumer client provides an interface to consume the records in a TDengine table or a valid
 * SQL query's result set.</p>
 * <p>The TaosConsumer client utilizes TDengine's JDBC module to establish and manage connections to a TDengine
 * cluster. To get connected to a TDengine cluster, necessary server information shall be provided in a Properties
 * object, see the usage of {@Link #subscribe() subscribe} method for details.</p>
 * <p>The methods in this consumer client are NOT thread safe. Also as per design, each consumer should subscribe
 * to a single topic, be it a table or a SQL query. Subscribing to multiple topics for a single consumer is
 * currently not supported.</p>
 * <p>Failure to close the consumer after use will cause connection leakage.</p>
 */
public class TaosConsumer implements Consumer {

    private static final String DEFAULT_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String DRIVER_PREFIX = "jdbc:TAOS://";
    private static final String DEFAULT_PORT = "0";

    private Connection connection;
    private String host;
    private String database;
    private String port;
    private Properties connectionProperties;
    private long queryInterval = 0L;
    private long maxPollSize = 10000L; // bytes
    private long lastKey = -1;
    private Topic topic;
    private ConsumerResultSet consumerResultSet;
    private Map<String, Long> lastKeys = new HashMap<String, Long>();

    private static final Logger log = LoggerFactory.getLogger(TaosConsumer.class);

    /**
     * A TaosConsumer is initiallized by providing a set of configuration properties. The necessary properties
     * includes: host, database, port, user, password, charset, locale, timezone, cfgdir.
     * The full list of supported keys and some detailed explanation are here:
     * "host", the host IP address of the TDengine cluster, usually it is a public IP of one of the servers in the cluster;
     * "database", the name of the database to connect to;
     * "port", the port of the TDengine server, "0" in most cases;
     * "user", the name of a valid TDengine user;
     * "password", the valid password for the given user;
     * "charset", an optional parameter, basically to tell the server what encoding charset the client machine uses. The defaut is UTF-8.
     * "locale", an optioanl parameter, tells the server what locale the client machine is using
     * "timezone", an optional paramter, tells the server what timezone the client machine is in.
     *
     * @param props
     */
    public TaosConsumer(Properties props) {
        try {
            parseConsumerProperties(props);
        } catch (Throwable t) {
            t.printStackTrace();
            log.error("Failed to construct TaosConsumer", t);
            throw new TaosRuntimeException("Failed to construct TaosConsumer", t);
        }
    }

    /**
     * Get the topic that the consumer client currently subscribes to.
     * @return a Topic obejct if the consumer has subscribed to it, otherwise null.
     */
    public Topic getTopic() {
        return this.topic;
    }

    /**
     * Subscribe to a given topic. A Topic object, could be a table, or a valid SQL query's result set in TDengine.
     * This method distinguishes different topics by their implementation type. Current supported topic types include:
     * TableTopic and SqlTopic.
     * This method will connect to TDengine with the provided connection properties in the TaosConsumer constructor.
     *
     * @param topic The topic to subscribe to.
     * @throws Exception
     */
    public void subscribe(Topic topic) throws Exception{
        try {
            connectToServer();
            if (connection == null) {
                throw new TaosRuntimeException("Failed to get connection");
            } else {
                SqlSyntaxValidator validater = new SqlSyntaxValidator(connection);
                if (!validater.validateSqlSyntax(topic.getTopicContent())) {
                    throw new TaosRuntimeException("Invalid sql");
                }
                this.topic = topic;
            }
            log.debug("Subscribe to topic: {}", topic.getTopicContent());
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to subscribe to the given topic", e);
            throw new Exception ("Failed to subscribe to the given topic");
        }
    }

    /**
     * Fetch data for the topic specified.
     * @param timeout
     * @param queryInterval, unit in milliseconds, is the time interval between two consecutive queries in a single
     *                       polling, default value is 0 millisecond.
     * @param maxPollSize, a limit on the number of rows returned in a single polling, the default is 10000.
     */
    public ConsumerResultSet poll(long timeout, long queryInterval, long maxPollSize) {
        this.queryInterval = queryInterval;
        this.maxPollSize = maxPollSize;
        return poll(timeout);
    }

    /**
     * Fetch data for the topic specified.
     * @param timeout
     * @param queryInterval, unit in milliseconds, is the time interval between two consecutive queries in a single
     *                       polling, default value is 0 millisecond.
     */
    public ConsumerResultSet poll(long timeout, long queryInterval) {
        this.queryInterval = queryInterval;
        return poll(timeout);
    }

    /**
     * Fetch data for the topic specified. This method is a synchronous polling process, which will block the current
     * thread and try to query the topic for any newly inserted records since last poll, until a non-empty result set
     * is retrieved or the processing time exceeds the timeout limit. The first time poll(long timeout) is called,
     * the consumer will try to fetch all records that have been newly inserted from the current system time. The
     * next call to poll() will start to fetch data that are newer than the latest record in the last call.
     *
     * @param timeout timeout for the polling process, unit in milliseconds
     * @return A ConsumerResultSet that contains the retrieved records
     */
    public ConsumerResultSet poll(long timeout) {
        log.debug("Polling starts.");
        long starttime = System.currentTimeMillis();
        if (lastKey < 0) {
            lastKey = starttime;
        }
        long timeUsed;
        long qStarttime;
        StringBuilder sql = new StringBuilder();
        try {

            consumerResultSet = new ConsumerResultSet();
            if (topic instanceof TableTopic) {
                Statement stmt = connection.createStatement();
                sql = new StringBuilder(((TableTopic) topic).getSelectClause());
                sql.append(((TableTopic) topic).getTableName())
                        .append(((TableTopic) topic).getWhereClause())
                        .append("_c0 > ")
                        .append(lastKey);
                if (!((TableTopic) topic).getSqlTail().isEmpty()) {
                    sql.append(" and ").append(((TableTopic) topic).getSqlTail());
                }

                log.debug("Executing sql: {}", sql.toString());
                while (true) {
                    qStarttime = System.currentTimeMillis();
                    System.out.println(sql.toString());
                    TSDBResultSet resSet = (TSDBResultSet) stmt.executeQuery(sql.toString());
                    if (resSet.getResultSetPointer() != 0) {
                        if (consumerResultSet.getResultSetMetaData() == null) {
                            consumerResultSet.setResultSetMetaData((TSDBResultSetMetaData) resSet.getMetaData());
                        }
                        while (resSet.next()) {
                            consumerResultSet.addRow(resSet.getRowData());
                        }
                        resSet.close();
                        if (consumerResultSet.size() > 0) {
                            log.debug("Retrieved {} records from executing sql: {}", consumerResultSet.size(), sql.toString());
                            lastKey = consumerResultSet.getRow(consumerResultSet.size() - 1).getLong(0, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP);
                            lastKeys.put(((TableTopic) topic).getTableName(), lastKey);
                            return consumerResultSet;
                        }
                    } else {
                        resSet.close();
                    }
                    if (System.currentTimeMillis() - starttime >= timeout) {
                        log.debug("Polling is timed out.");
                        return consumerResultSet;
                    }
                    timeUsed = System.currentTimeMillis() - qStarttime;
                    if (timeUsed < queryInterval) {
                        Thread.currentThread().sleep(queryInterval - timeUsed);
                    }
                }
            } else if (topic instanceof STableTopic) {
                // update map lastKeys
                Statement stmt0 = connection.createStatement();
                sql = new StringBuilder("select tbname from ")
                        .append(((STableTopic) topic).getTableName())
                        .append(((STableTopic) topic).getWhereClause())
                        .append(((STableTopic) topic).getSqlTail());
                ResultSet resSet = stmt0.executeQuery(sql.toString());
                ResultSetMetaData resSetMetaData = resSet.getMetaData();
                Map<String, Long> newLastKeyMap = new HashMap<>();
                String tbname;
                while (resSet.next()) {
                    tbname = resSet.getString(1);
                    if (tbname == null || tbname.isEmpty()) {
                        log.error("No tables found under super table {}", ((STableTopic) topic).getTableName());
                    } else {
                        if (lastKeys.get(tbname) == null) {
                            // newly added tables since the last poll
                            newLastKeyMap.put(tbname, starttime);
                        } else {
                            // tables already existed in last poll
                            newLastKeyMap.put(tbname, lastKeys.get(tbname));
                        }
                    }
                }
                lastKeys = newLastKeyMap;
                resSet.close();

                boolean hasRetrievedData = false;
                long rowsAvailable = maxPollSize;
                long tablesRemaining;
                long maxRowsPerRetrival;
                long rowCounter = 0L;

                while (true) {
                    System.out.println("stb query");
                    qStarttime = System.currentTimeMillis();
                    tablesRemaining = lastKeys.size();
                    maxRowsPerRetrival = tablesRemaining > 0 ? (maxPollSize/tablesRemaining): 0L;
                    if (lastKeys.size() < 1) {
                        if (System.currentTimeMillis() - starttime >= timeout) {
                            log.debug("Polling is timed out.");
                            return consumerResultSet;
                        }
                        timeUsed = System.currentTimeMillis() - qStarttime;
                        if (timeUsed < queryInterval) {
                            Thread.currentThread().sleep(queryInterval - timeUsed);
                        }
                        continue;
                    } else {
                        List<TSDBResultSet> tsdbResultSetList = new ArrayList<>(lastKeys.size());
                        for (Map.Entry<String, Long> entry : lastKeys.entrySet()) {
                            Statement stmt = connection.createStatement();
                            sql = new StringBuilder(((STableTopic) topic).getSelectClause())
                                    .append(((STableTopic)topic).getTableName())
                                    .append(((STableTopic) topic).getWhereClause())
                                    .append("_c0 > ")
                                    .append(entry.getValue())
                                    .append(" and tbname = '")
                                    .append(entry.getKey())
                                    .append("' and")
                                    .append(((STableTopic) topic).getSqlTail());
                            TSDBResultSet resultSet = (TSDBResultSet) stmt.executeQuery(sql.toString());
                            if (resultSet.getResultSetPointer() != 0) {
                                if (consumerResultSet.getResultSetMetaData() == null) {
                                    consumerResultSet.setResultSetMetaData((TSDBResultSetMetaData) resultSet.getMetaData());
                                }
                                rowCounter = 0;
                                maxRowsPerRetrival = Long.divideUnsigned(rowsAvailable - rowCounter, tablesRemaining);
                                if (maxRowsPerRetrival < 1) {
                                    maxRowsPerRetrival = 1;
                                }
                                while (resultSet.next() && rowCounter <= maxRowsPerRetrival) {
                                    consumerResultSet.addRow(resultSet.getRowData());
                                    rowCounter++;
                                }
                                resSet.close();
                                if (consumerResultSet.size() > 0) {
                                    log.debug("Retrieved {} records from executing sql: {}", consumerResultSet.size(), sql.toString());
                                    lastKey = consumerResultSet.getRow(consumerResultSet.size() - 1).getLong(0, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP);
                                    lastKeys.put(entry.getKey(), lastKey);
                                    hasRetrievedData = true;
                                }
                            } else {
                                resSet.close();
                            }
                            tablesRemaining--;
                        }
                        if (hasRetrievedData) {
                            return consumerResultSet;
                        } else {
                            // sleep
                            if (System.currentTimeMillis() - starttime >= timeout) {
                                log.debug("Polling is timed out.");
                                return consumerResultSet;
                            }
                            timeUsed = System.currentTimeMillis() - qStarttime;
                            if (timeUsed < queryInterval) {
                                Thread.currentThread().sleep(queryInterval - timeUsed);
                            }
                        }
                    }
                }
            } else {
                throw new TaosRuntimeException("Invalid topic type: + " + topic.getClass().getName());
            }

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to retrieve data from server", e);
            log.error("Failed when executing SQL: {}", sql.toString());
            throw new TaosRuntimeException("Failed to retrieve data from server", e);
        }
    }


    /**
     * Reset the polling start time to the given time.
     * @param startTime
     */
    @Override
    public void seekTo(long startTime) {
        this.lastKey = startTime;
    }

    /**
     * Reset the polling start time to 0, meaning the next polling will try to retrieve all the records in the subscribed
     * topic.
     */
    @Override
    public void seekToBeginning() {
        this.lastKey = 0;
    }

    /**
     * Unsubscribe to the topic. This process will clear all the information related to the last topic.
     * But it keeps the JDBC connection.
     */
    @Override
    public void unsubscribe() {
        log.debug("Unsubscribe topic: {}", this.topic.getTopicContent());
        try {
            this.lastKey = -1;
            this.topic = null;

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to unsubcribe to topic {}", topic.getTopicContent());
            throw new TaosRuntimeException("Failed to unsubscribe");
        }
    }

    /**
     * Retrieve the position of last consumed record
     * @return a Unix timestamp which is the primary key for the last consumed record
     */
    @Override
    public long position() {
        return this.lastKey;
    }

    /**
     * Close the consumer and the TDengine connection.
     */
    @Override
    public void close() {
        unsubscribe();
        try {
            this.connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to close the JDBC connection in the consumer.", e);
        }
    }

    /**
     * Get the query interval in the poll() method of this consumer. In a polling process, the consumer will continue
     * to query the database for new data until it fetches a non-empty result set. The query interval determines the
     * frequency of retry queries if the last query retrieved an empty result set.
     * @return the query interval in milliseconds
     */
    public long getQueryInterval() {
        return this.queryInterval;
    }

    /**
     * Set the query interval in the poll() method of this consumer
     * @param queryInterval
     */
    public void setQueryInterval(long queryInterval) {
        this.queryInterval = queryInterval;
    }

    /**
     * Get the maximum number of rows to retrieve in a single polling
     * @return max number of rows that a single polling can retrieve
     */
    public long getMaxPollSize() {
        return this.maxPollSize;
    }

    /**
     * Set the maximum number of rows to retrieve in a single polling
     * @param maxPollSize
     */
    public void setMaxPollSize(long maxPollSize) {
        this.maxPollSize = maxPollSize;
    }

    /**
     * Parse the input consumer configuration properties and validate if all necessary properties are present
     * @param props
     * @return
     * @throws Exception
     */
    private void parseConsumerProperties(Properties props) throws Exception{

        this.connectionProperties = new Properties();
        if (props.getProperty("host") != null && props.getProperty("host").length() > 0) {
            this.host = props.getProperty("host");
            this.connectionProperties.setProperty("host",props.getProperty("host"));
        } else {
            throw new ConsumerConfigException("Configuration property 'host' is not found");
        }
        if (props.getProperty("database") != null && props.getProperty("host").length() > 0) {
            this.database = props.getProperty("database");
            this.connectionProperties.setProperty("database",props.getProperty("database"));
        } else {
            throw new ConsumerConfigException("Configuration property 'database' is not found");
        }
        if (props.getProperty("port") != null && props.getProperty("port").length() > 0) {
            this.port = props.getProperty("port");
            this.connectionProperties.setProperty("port",props.getProperty("port"));
        } else {
            this.port = "0";
        }
        if (props.getProperty("user") != null && props.getProperty("user").length() > 0) {
            this.connectionProperties.setProperty("user",props.getProperty("user"));
        } else {
            throw new ConsumerConfigException("Configuration property 'user' is not found");
        }
        if (props.getProperty("password") != null && props.getProperty("password").length() > 0) {
            this.connectionProperties.setProperty("password",props.getProperty("password"));
        } else {
            throw new ConsumerConfigException("Configuration property 'password' is not found");
        }
        if (props.getProperty("charset") != null && props.getProperty("charset").length() > 0) {
            this.connectionProperties.setProperty("charset",props.getProperty("charset"));
        }
        if (props.getProperty("locale") != null && props.getProperty("locale").length() > 0) {
            this.connectionProperties.setProperty("locale",props.getProperty("locale"));
        }
        if (props.getProperty("timezone") != null && props.getProperty("timezone").length() > 0) {
            this.connectionProperties.setProperty("timezone",props.getProperty("timezone"));
        }
        if (props.getProperty("cfgdir") != null && props.getProperty("cfgdir").length() > 0) {
            this.connectionProperties.setProperty("cfgdir", props.getProperty("cfgdir"));
        } else {
            this.connectionProperties.setProperty("cfgdir", "/etc/taos");
        }

    }

    /**
     * Connect to TDengine cluster through JDBC connections. The necessary connection parameters should be passed
     * in the constructor. Clients on Windows OS should provide the correct encoding charset.
     */
    private void connectToServer() {
        try {
            Class.forName(DEFAULT_DRIVER);
            String url = DRIVER_PREFIX + host + ":" + port + "/" + database;
            connection = DriverManager.getConnection(url, connectionProperties);
            log.debug("Connection to TDengine server is established.");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to connect to server: host={}, database={}, port={}, user={}, charset={}, locale={}, timezone={}",
                    host, database, port, connectionProperties.getProperty("user"), connectionProperties.getProperty("charset"),
                    connectionProperties.getProperty("locale"), connectionProperties.getProperty("timezone"));
            throw new TaosRuntimeException("Failed to connect to server!", e);
        }
    }
}
