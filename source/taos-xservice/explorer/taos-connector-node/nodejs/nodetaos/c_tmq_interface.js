// bind c interface with user APIs

const CTaosInterface = require('./cinterface');
const ffi = require('ffi-napi');
const ref = require('ref-napi');
const libtmq = require('./tmqlib');
const libtaos = require('./taoslib');
const errors = require('./error');
const { ConsumerResult, TopicPartition } = require('./consumerResult');

module.exports = CTMQInterface;

function CTMQInterface(config) {
    // this.consumer = this._newConsumer(config);
    return this;
}
CTMQInterface.prototype.newConsumer = function newConsumer(config) {
    let tmqCfg = libtmq.tmq_conf_new();
    try {
        for (var [key, val] of config) {
            let confRes = libtmq.tmq_conf_set(tmqCfg, ref.allocCString(key), ref.allocCString(val));
            if (confRes == -2) {
                throw new errors.TMQError("TMQ_CONF_UNKNOWN," + key + "=" + val);
            } else if (confRes == -1) {
                throw new errors.TMQError("TMQ_CONF_INVALID," + key + "=" + val);
            }
        }

        let errStrLength = 512;
        let errorStrPtr = ref.ref(Buffer.alloc(errStrLength));

        let consumer = libtmq.tmq_consumer_new(tmqCfg, errorStrPtr, errStrLength);
        if (consumer.isNull()) {
            let errorStr = ref.readCString(errorStrPtr);
            throw new errors.TMQError(`new consumer failed,reason:${errorStr}`);
        }
        return consumer;
    }
    finally {
        libtmq.tmq_conf_destroy(tmqCfg);
    }

}

CTMQInterface.prototype.subscribe = function subscribe(consumer, topics) {
    let tmqListPtr = libtmq.tmq_list_new();
    try {
        let topicList = [];
        if (Array.isArray(topics)) {
            topicList = topics;
        } else {
            topicList.push(topics);
        }
        topicList.forEach(item => {
            let listRes = libtmq.tmq_list_append(tmqListPtr, ref.allocCString(item));
            if (listRes != 0) {
                throw new errors.TMQError("set topic " + item + "failed")
            }
        })

        let code = libtmq.tmq_subscribe(consumer, tmqListPtr);
        this._consumerErrorHandle("subscribe failed", code)
    } finally {
        libtmq.tmq_list_destroy(tmqListPtr);
    }
}

CTMQInterface.prototype.subscription = function subscription(consumer) {
    let topicList = [];
    let topicPtr = libtmq.tmq_list_new();
    let topicPPtr = ref.ref(topicPtr);

    let code = libtmq.tmq_subscription(consumer, topicPPtr);
    this._consumerErrorHandle("tmq_subscription", code);

    // let topic = ref.reinterpret(topicPPtr,ref.sizeof.pointer)
    let c_tmq_list_length = libtmq.tmq_list_get_size(topicPtr);
    let c_topic_list_ptr = libtmq.tmq_list_to_c_array(topicPtr);
    // console.log(c_tmq_list_length);

    for (let i = 0; i < c_tmq_list_length; i++) {
        let tpcPtr = ref.readPointer(c_topic_list_ptr, i * ref.sizeof.pointer, ref.sizeof.pointer);
        let tpc = ref.readCString(tpcPtr);
        // let tpc = ref.readCString(ref.deref(c_topic_list_ptr));
        topicList.push(tpc)
    }

    libtmq.tmq_list_destroy(topicPtr);
    return topicList;
}

CTMQInterface.prototype.consume = function consume(consumer, timeout) {
    let ctaos = new CTaosInterface();
    let taosRes = libtmq.tmq_consumer_poll(consumer, timeout);

    let consumerResult = new ConsumerResult(taosRes);

    while (true) {
        let rawBlock = ctaos.fetchRawBlock(taosRes)

        if (rawBlock.num_of_rows == 0) {
            break;
        } else {
            let data = []
            //data = rawBlock.blocks;
            let topic = ref.readCString(libtmq.tmq_get_topic_name(taosRes));
            let db = ref.readCString(libtmq.tmq_get_db_name(taosRes));
            let vGroupId = libtmq.tmq_get_vgroup_id(taosRes);
            let table = ref.readCString(libtmq.tmq_get_table_name(taosRes));


            let topicPartition = new TopicPartition(topic, db, vGroupId, table)
            let fields = ctaos.useResult(taosRes);


            for (let i = 0; i < rawBlock.num_of_rows; i++) {
                for (let j = 0; j < fields.length; j++) {
                    data.push(rawBlock.blocks[j][i]);
                }
            }
            consumerResult.set(topicPartition, data, fields);
        }

    }
    return consumerResult;
}
CTMQInterface.prototype.unsubscribe = function unsubscribe(consumer) {
    let code = libtmq.tmq_unsubscribe(consumer);
    this._consumerErrorHandle("unsubscribe", code);
}
CTMQInterface.prototype.consumerClose = function consumerClose(consumer) {
    let code = libtmq.tmq_consumer_close(consumer);
    this._consumerErrorHandle("consumer close", code);
}

CTMQInterface.prototype.commit = function commit(consumer, consumerResult) {
    let code = libtmq.tmq_commit_sync(consumer, consumerResult.msg);
    this._consumerErrorHandle("commit", code);
    consumerResult.close();
}

CTMQInterface.prototype.commit_a = function commit_a(consumer, consumerResult, callback, param = ref.ref(ref.NULL)) {

    var fetchRawBlock_a_callback = function (consumer2, code2, param2) {
        // something to do
        callback(consumer2, code2, param2);
    }
    var fetchRawBlock_a_callback = ffi.Callback(ref.types.value, [ref.refType(ref.types.void), ref.types.int32, ref.refType(ref.types.void)], fetchRawBlock_a_callback);
    libtmq.tmq_commit_async(consumer, ref.ref(ref.NULL), fetchRawBlock_a_callback, param);
}

CTMQInterface.prototype._consumerErrorHandle = function _consumerErrorHandle(msg, code) {
    if (code == 0) {
        return;
    } else {
        throw new errors.TMQError(`${msg} failed,reason ${libtmq.tmq_err2str(code)} code:${code}`);
    }
}
