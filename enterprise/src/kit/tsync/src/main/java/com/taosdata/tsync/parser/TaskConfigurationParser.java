package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.*;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.entity.config.TaskConfiguration;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TaskConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(TaskConfigurationParser.class);

    private final ConfigurationType type = ConfigurationType.TASK;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        TaskConfiguration config = new TaskConfiguration();
        // topic
        if (configJSON.containsKey("topic")) {
            String topic = configJSON.getString("topic");
            config.setTopic(topic);
        } else {
            String errorMsg = "topic is necessary in Task Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        if (configJSON.containsKey("partitions")) {
            JSONArray arr = configJSON.getJSONArray("partitions");
            int[] partitions = buildPartitionsArray(arr);
            config.setPartitions(partitions);
        }

        // TODO: threads is not necessary
        if (configJSON.containsKey("threads")) {
            Integer threads = configJSON.getInteger("threads");
            config.setThreads(threads);
        }
        return config;
    }

    private int[] buildPartitionsArray(JSONArray array) {
        List<Integer> partitions = new ArrayList<>();

        RangeSet<Integer> rangeSet = TreeRangeSet.create();
        for (int i = 0; i < array.size(); i++) {
            Range<Integer> range = Utils.closedRange(array.getString(i));
            Range<Integer> canonical = range.canonical(DiscreteDomain.integers());
            rangeSet.add(canonical);
        }
        Set<Range<Integer>> ranges = rangeSet.asRanges();
        for (Range<Integer> range : ranges) {
            for (int p : ContiguousSet.create(range, DiscreteDomain.integers())) {
                partitions.add(p);
            }
        }

        return partitions.stream().filter(i -> i >= TaskConfiguration.MIN_PARTITION_INDEX && i <= TaskConfiguration.MAX_PARTITION_INDEX).mapToInt(Integer::intValue).toArray();
    }


}
