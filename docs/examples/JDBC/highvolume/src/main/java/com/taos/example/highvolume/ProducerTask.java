/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.taos.example.highvolume;

import com.taosdata.jdbc.utils.ReqId;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Properties;

class ProducerTask implements Runnable, Stoppable {
    private final static Logger logger = LoggerFactory.getLogger(ProducerTask.class);
    private final int taskId;
    private final int subTableStartIndex;
    private final int subTableEndIndex;
    private final int rowsPerTable;
    private volatile boolean  active = true;
    public ProducerTask(int taskId,
                        int rowsPerTable,
                        int subTableStartIndex,
                        int subTableEndIndex) {
        this.taskId = taskId;
        this.subTableStartIndex = subTableStartIndex;
        this.subTableEndIndex = subTableEndIndex;
        this.rowsPerTable = rowsPerTable;
    }

    @Override
    public void run() {
        logger.info("kafak producer {}, started", taskId);
        Iterator<Meters> it = new MockDataSource(subTableStartIndex, subTableEndIndex, rowsPerTable);

        Properties props = new Properties();
        props.put("bootstrap.servers", Util.getKafkaBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("batch.size", 1024 * 1024);
        props.put("linger.ms", 500);

        // create a Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            while (it.hasNext() && active) {
                Meters meters = it.next();
                String key = meters.getTableName();
                String value = meters.toString();
                // to avoid the data of the sub-table out of order. we use the partition key to ensure the data of the same sub-table is sent to the same partition.
                // Because efficient writing use String hashcode，here we use another hash algorithm to calculate the partition key.
                long hashCode = Math.abs(ReqId.murmurHash32(key.getBytes(), 0));
                ProducerRecord<String, String> metersRecord = new ProducerRecord<>(Util.getKafkaTopic(), (int)(hashCode % Util.getPartitionCount()), key, value);
                producer.send(metersRecord);
            }
        } catch (Exception e) {
            logger.error("task id {}, send message error: ", taskId, e);
        }
        finally {
            producer.close();
        }
        logger.info("kafka producer {} stopped", taskId);
    }

    public void stop() {
        logger.info("kafka producer {} stopping", taskId);
        this.active = false;
    }
}