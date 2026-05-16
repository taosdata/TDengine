package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL;
import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_TMQ_CONSUMER_NULL;
@SuppressWarnings("java:S125")
public class TMQConnector extends TSDBJNIConnector {

    private String createConsumerErrorMsg;
    private String[] topics;

    public long createConfig(Properties properties) throws SQLException {
        long conf = tmqConfNewImp();
        if (null == properties || properties.size() < 1)
            return conf;
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (!StringUtils.isEmpty(key) && !TMQConstants.knownKeys.contains(key)) {
                int code = tmqConfSetImp(conf, key, String.valueOf(entry.getValue()));
                if (code == TMQ_CONF_KEY_NULL) {
                    tmqConfDestroyImp(conf);
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_CONF_KEY_NULL);
                }
                if (code == TMQ_CONF_VALUE_NULL) {
                    tmqConfDestroyImp(conf);
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_CONF_VALUE_NULL, "failed to set consumer property, " + key + "'s value is null");
                }
                if (code < TMQ_SUCCESS) {
                    tmqConfDestroyImp(conf);
                    throw TSDBError.createSQLException(code,
                            "failed to set consumer property, " + key + ":" + entry.getValue() + ", reason: " + getErrMsg(code));
                }
            }
        }
        return conf;
    }

    // DLL_EXPORT tmq_conf_t *tmq_conf_new();
    private native long tmqConfNewImp();

    // DLL_EXPORT tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf,
    // const char *key, const char *value);
    private native int tmqConfSetImp(long conf, String key, String value);

    public void destroyConf(long conf) {
        tmqConfDestroyImp(conf);
    }

    // DLL_EXPORT void tmq_conf_destroy(tmq_conf_t *conf);
    private native void tmqConfDestroyImp(long conf);

    public void createConsumer(long conf) throws SQLException {
        taos = tmqConsumerNewImp(conf, this);
        if (taos == TMQ_CONF_NULL) {
            throw TSDBError.createSQLException(TMQ_CONF_NULL);
        }
        if (taos == JNI_OUT_OF_MEMORY) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_OUT_OF_MEMORY);
        }
        if (taos < TMQ_SUCCESS) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_CONSUMER_CREATE_ERROR, createConsumerErrorMsg);
        }
    }

    // DLL_EXPORT tmq_t *tmq_consumer_new(tmq_conf_t *conf,
    // char *errstr, int32_t errstrLen);
    private native long tmqConsumerNewImp(long conf, TMQConnector connector);

    void setCreateConsumerErrorMsg(String msg) {
        this.createConsumerErrorMsg = msg;
    }

    public long createTopic(Collection<String> topics) throws SQLException {
        long topic = tmqTopicNewImp(taos);
        if (topic < TMQ_SUCCESS) {
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL);
        }
        if (null != topics && !topics.isEmpty()) {
            for (String name : topics) {
                int code = tmqTopicAppendImp(topic, name);
                if (code == TMQ_TOPIC_NULL) {
                    destroyTopic(topic);
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NULL);
                }
                if (code == TMQ_TOPIC_NAME_NULL) {
                    destroyTopic(topic);
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NAME_NULL);
                }
                if (code != TMQ_SUCCESS) {
                    destroyTopic(topic);
                    throw TSDBError.createSQLException(code, getErrMsg(code));
                }
            }
        }
        return topic;
    }

    // DLL_EXPORT tmq_list_t *tmq_list_new();
    private native long tmqTopicNewImp(long tmq);

    // DLL_EXPORT int32_t tmq_list_append(tmq_list_t *, const char *);
    private native int tmqTopicAppendImp(long topic, String topicName);

    public void destroyTopic(long topic) {
        tmqTopicDestroyImp(topic);
    }

    // DLL_EXPORT void tmq_list_destroy(tmq_list_t *);
    private native void tmqTopicDestroyImp(long topic);

    public void subscribe(long topic) throws SQLException {
        int code = tmqSubscribeImp(taos, topic);
        if (code == TMQ_CONSUMER_NULL)
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL, "failed to subscribe topic, consumer reference has been destroyed");

        if (code == TMQ_TOPIC_NULL)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NULL);

        if (code != TMQ_SUCCESS)
            throw TSDBError.createSQLException(code, getErrMsg(code));
    }

    // DLL_EXPORT int32_t tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list);
    private native int tmqSubscribeImp(long tmq, long topic);

    public Set<String> subscription() throws SQLException {
        int code = tmqSubscriptionImp(taos, this);
        if (code == TMQ_CONSUMER_NULL)
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL, "get subscription error, consumer reference has been destroyed");

        if (code != TMQ_SUCCESS)
            throw TSDBError.createSQLException(code, getErrMsg(code));

        return Arrays.stream(topics).collect(Collectors.toSet());
    }

    // DLL_EXPORT int32_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics);
    private native int tmqSubscriptionImp(long tmq, TMQConnector connector);

    public void setTopicList(String[] topics) {
        this.topics = topics;
    }

    public void syncCommit() throws SQLException {
        int code = tmqCommitAllSync(taos);
        if (code == TMQ_CONSUMER_NULL)
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL, "sync commit offset error, consumer reference has been destroyed");

        if (code != TMQ_SUCCESS)
            throw TSDBError.createSQLException(code, createConsumerErrorMsg);
    }

    // commit all
    private native int tmqCommitAllSync(long tmq);

    public void commitOffsetSync(String topicName, int vgId, long offset) throws SQLException {
        int code = tmqCommitOffsetSyncImp(this.taos, topicName, vgId, offset);
        if (code != TMQ_SUCCESS) {
            if (code == TMQ_CONSUMER_NULL) {
                throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL);
            } else if (code == TMQ_TOPIC_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NULL);
            }
            throw TSDBError.createSQLException(code, getErrMsg(code));
        }
    }

    // DLL_EXPORT int32_t   tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset);
    private native int tmqCommitOffsetSyncImp(long tmq, String topicName, int vgId, long offset);

    public void asyncCommit(OffsetWaitCallback<?> callback) {
        consumerCommitAllAsync(taos, callback);
    }

    private native void consumerCommitAllAsync(long tmq, OffsetWaitCallback<?> callback);

    public void asyncCommit(String topicName, int vgId, long offset, OffsetWaitCallback<?> callback) {
        if (taos == 0) {
            throw TSDBError.createIllegalArgumentException(ERROR_TMQ_CONSUMER_NULL);
        }
        if (topicName == null || topicName.isEmpty()) {
            throw TSDBError.createIllegalArgumentException(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NULL);
        }

        consumerCommitOffsetAsync(taos, topicName, vgId, offset, callback);
    }

    // DLL_EXPORT void      tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb, void *param);
    private native void consumerCommitOffsetAsync(long tmq, String topicName, int vgId, long offset, OffsetWaitCallback<?> callback);

    public void unsubscribe() throws SQLException {
        int code = tmqUnsubscribeImp(taos);
        if (code == TMQ_CONSUMER_NULL)
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL, "unsubscribe error, consumer reference has been destroyed");

        if (code != TMQ_SUCCESS)
            throw TSDBError.createSQLException(code, getErrMsg(code));
    }

    // DLL_EXPORT int32_t tmq_unsubscribe(tmq_t *tmq);
    private native int tmqUnsubscribeImp(long tmq);

    public void closeConsumer() throws SQLException {
        int code = tmqConsumerCloseImp(taos);
        if (code != TMQ_SUCCESS && code != TMQ_CONSUMER_NULL)
            throw TSDBError.createSQLException(code, getErrMsgImp(code));
    }

    // DLL_EXPORT int32_t tmq_consumer_close(tmq_t *tmq);
    private native int tmqConsumerCloseImp(long tmq);


    // TODO confirm code range, which is applied to
    public String getErrMsg(int code) {
        return this.getErrMsgImp(code);
    }

    // DLL_EXPORT const char *tmq_err2str(int32_t code);
    private native String getErrMsgImp(int code);

    public long poll(long waitTime) throws SQLException {
        long l = tmqConsumerPoll(taos, waitTime);
        if (l == TMQ_CONSUMER_NULL)
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL);

        return l;
    }

    // DLL_EXPORT TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t wait_time);
    private native long tmqConsumerPoll(long tmq, long waitTime);


    public String getTopicName(long res) throws SQLException {
        String s = tmqGetTopicName(res);
        if (s == null)
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL);

        return s;
    }

    // DLL_EXPORT const char *tmq_get_topic_name(TAOS_RES *res);
    private native String tmqGetTopicName(long res);

    public String getDbName(long res) throws SQLException {
        String s = tmqGetDbName(res);
        if (s == null)
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL);

        return s;
    }

    // DLL_EXPORT const char *tmq_get_db_name(TAOS_RES *res);
    private native String tmqGetDbName(long res);

    public int getVgroupId(long res) throws SQLException {
        int code = tmqGetVgroupId(res);
        if (code == JNI_RESULT_SET_NULL)
            throw TSDBError.createSQLException(ERROR_JNI_RESULT_SET_NULL);

        return code;
    }

    // DLL_EXPORT int32_t tmq_get_vgroup_id(TAOS_RES *res);
    private native int tmqGetVgroupId(long res);

    public String getTableName(long res) {
        return tmqGetTableName(res);
    }

    // DLL_EXPORT const char *tmq_get_table_name(TAOS_RES *res);
    private native String tmqGetTableName(long res);

    // DLL_EXPORT int64_t     tmq_get_vgroup_offset(TAOS_RES* res);
    private native long tmqGetOffset(long res);

    public long getOffset(long res) throws SQLException {
        long l = tmqGetOffset(res);
        if (l == JNI_RESULT_SET_NULL)
            throw TSDBError.createSQLException(ERROR_JNI_RESULT_SET_NULL);

        return l;
    }

    public int fetchBlock(long resultSet, TSDBResultSetBlockData blockData, List<ColumnMetaData> columnMetaData) {
        int ret = this.fetchRawBlockImp(this.taos, resultSet, blockData, columnMetaData);
        columnMetaData.forEach(column -> column.setColIndex(column.getColIndex() + 1));
        return ret;
    }

    private native int fetchRawBlockImp(long connection, long resultSet, TSDBResultSetBlockData blockData, List<ColumnMetaData> columnMetaData);

    public void seek(String topicName, int vgId, long offset) {
        int code = tmqSeekImp(this.taos, topicName, vgId, offset);
        if (code != TMQ_SUCCESS) {
            if (code == TMQ_CONSUMER_NULL) {
                throw TSDBError.createRuntimeException(ERROR_TMQ_CONSUMER_NULL);
            } else if (code == TMQ_TOPIC_NULL) {
                throw TSDBError.createRuntimeException(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NULL);
            }
            throw TSDBError.createRuntimeException(code, getErrMsg(code));
        }
    }

    // DLL_EXPORT int32_t   tmq_offset_seek(tmq_t *tmq, const char* pTopicName, int32_t vgId, int64_t offset);
    private native int tmqSeekImp(long tmq, String topicName, int vgId, long offset);

    public List<Assignment> getTopicAssignment(String topicName) {
        List<Assignment> assignments = new ArrayList<>();
        int code = tmqGetTopicAssignmentImp(this.taos, topicName, assignments);
        if (code != TMQ_SUCCESS) {
            if (code == TMQ_CONSUMER_NULL) {
                throw TSDBError.createRuntimeException(ERROR_TMQ_CONSUMER_NULL);
            } else if (code == TMQ_TOPIC_NULL) {
                throw TSDBError.createRuntimeException(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NULL);
            }
            throw TSDBError.createRuntimeException(code, getErrMsg(code));
        }

        return assignments;
    }

    // DLL_EXPORT int32_t   tmq_get_topic_assignment(tmq_t *tmq, const char* pTopicName, tmq_topic_assignment **assignment, int32_t *numOfAssignment);
    private native int tmqGetTopicAssignmentImp(long tmq, String topicName, List<Assignment> assignments);

    public long committed(String topicName, int vgId) throws SQLException {
        long l = tmqCommittedImp(this.taos, topicName, vgId);
        if (l == TMQ_CONSUMER_NULL) {
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL);
        } else if (l == TMQ_TOPIC_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NULL);
        }
        if (l < TMQ_SUCCESS && l != TMQConstants.INVALID_OFFSET)
            throw TSDBError.createSQLException((int) l, getErrMsg((int) l));

        return l;
    }

    // DLL_EXPORT int64_t     tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId)
    private native long tmqCommittedImp(long tmq, String topicName, int vgId);

    public long position(String topicName, int vgId) throws SQLException {
        long l = tmqPositionImp(this.taos, topicName, vgId);
        if (l == TMQ_CONSUMER_NULL) {
            throw TSDBError.createSQLException(ERROR_TMQ_CONSUMER_NULL);
        } else if (l == TMQ_TOPIC_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NULL);
        }
        if (l < TMQ_SUCCESS)
            throw TSDBError.createSQLException((int) l, getErrMsg((int) l));

        return l;
    }

    // DLL_EXPORT int64_t     tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId)
    private native long tmqPositionImp(long tmq, String topicName, int vgId);

}
