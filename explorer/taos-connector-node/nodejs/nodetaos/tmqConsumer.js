const errors = require('./error');
const CTMQInterface = require('./c_tmq_interface');
const CTaosInterface = require('./cinterface');
const { ConsumerResult } = require('./consumerResult');

function TMQConsumer(config) {

    this.config = new Map();
    this.config.set('group.id', 'null');
    this.config.set('client.id', 'null');
    this.config.set('enable.auto.commit', 'true');
    this.config.set('auto.commit.interval.ms', '1000');
    this.config.set('auto.offset.reset', 'earliest');
    this.config.set('msg.with.table.name', 'true');
    this.config.set('td.connect.ip', '127.0.0.1');
    this.config.set('td.connect.user', 'root');
    this.config.set('td.connect.pass', 'taosdata');
    this.config.set('td.connect.pass', 'taosdata');
    this.consumerResult = null;

    this._tmqConfig(config);
    this.c_tmq_handle = new CTMQInterface();
    this.c_taos_handle = new CTaosInterface();
    this.consumer = this.c_tmq_handle.newConsumer(this.config);
    return this;
}


TMQConsumer.prototype._tmqConfig = function _tmqConfig(config) {

    if (!config['group.id']) {
        throw new errors.TMQError("\'group.id\' must be defined.");
    }
    Reflect.ownKeys(config).map(key => {
        this.config.set(key, config[key]);
    })
}

TMQConsumer.prototype._msgClose = function () {
    if (this.consumerResult !== null) {
        this.c_taos_handle.freeResult(this.consumerResult.msg);
        this.consumerResult = null;
    }
}

/**
 * 
 * @param {*} topic 
 */
TMQConsumer.prototype.subscribe = function subscribe(topic) {
    this.c_tmq_handle.subscribe(this.consumer, topic);
}

TMQConsumer.prototype.unsubscribe = function unsubscribe() {
    this._msgClose();
    this.c_tmq_handle.unsubscribe(this.consumer);
}

TMQConsumer.prototype.subscription = function subscription() {
    return this.c_tmq_handle.subscription(this.consumer);
}

TMQConsumer.prototype.consume = function consume(timeout) {
    this.consumerResult = this.c_tmq_handle.consume(this.consumer, timeout);
    return this.consumerResult;
}
TMQConsumer.prototype.commit = function commit(msg) {
    this.c_tmq_handle.commit(this.consumer, msg);
}
TMQConsumer.prototype.commit_a = function commit_a(msg, callback) {
    this.c_tmq_handle.commit_a(this.consumer, msg, callback)
}
TMQConsumer.prototype.close = function close() {
    this._msgClose();
    this.c_tmq_handle.consumerClose(this.consumer);
}
module.exports = TMQConsumer;
