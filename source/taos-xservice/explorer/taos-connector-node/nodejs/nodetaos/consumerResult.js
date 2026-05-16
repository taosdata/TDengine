const libtmq = require('./tmqlib');


class ConsumerResult {
    constructor(taosRes) {
        this.topicPartition = [];
        this.block = [];
        this.fields = [];
        this.msg = taosRes;
    }

    set(topicPartition, data, field) {

        let containIndex = this._contains(topicPartition);
        if (containIndex > -1) {
            this.block[containIndex].concat(data);
        } else {
            this.topicPartition.push(topicPartition);
            this.block.push(data);
            this.fields.push(field);
        }
    }

    close() {
        this.msg = null;
    }

    _contains(topicPartition) {
        let key = -1;
        this.topicPartition.forEach((item, index) => {
            if (item.equals(topicPartition)) {
                key = index;
                return;
            }
        })
        return key;
    }

}

class TopicPartition {
    constructor(topic, db, vGroupId, table, taosRes) {
        this.topic = topic;
        this.db = db;
        this.vGroupId = vGroupId;
        this.table = table;
    }

    equals(topicPartition) {
        if (this.topic != topicPartition.topic) {
            return false;
        }
        if (this.db != topicPartition.db) {
            return false;
        }
        if (this.vGroupId != topicPartition.vGroupId) {
            return false;
        }
        if (this.table != topicPartition.table) {
            return false;
        }
        return true;
    }

}
module.exports = { ConsumerResult, TopicPartition };
