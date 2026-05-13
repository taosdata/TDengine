package com.taosdata.tsync.entity;

import com.taosdata.tsync.utils.DataGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TopicPartitionTest {

    @Test
    public void testHashCode() {
        // given
        int partitionCount = 1000;
        List<String> topics = IntStream.range(1, 100).mapToObj(DataGenerator::randomString).collect(Collectors.toList());

        // when and then
        topics.stream().forEach(topic -> {
            long total = IntStream.range(1, 1 + partitionCount).mapToObj(i -> TopicPartition.hashCode(topic, i)).distinct().count();
            Assert.assertEquals(partitionCount, total);
        });
    }
}