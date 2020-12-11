package com.taosdata.taosdemo.service.data;

import com.taosdata.taosdemo.domain.TagMeta;
import com.taosdata.taosdemo.domain.TagValue;
import com.taosdata.taosdemo.utils.DataGenerator;

import java.util.ArrayList;
import java.util.List;

public class TagValueGenerator {

    // 创建标签值：使用tagMetas
    public static List<TagValue> generate(List<TagMeta> tagMetas) {
        List<TagValue> tagValues = new ArrayList<>();
        for (int i = 0; i < tagMetas.size(); i++) {
            TagMeta tagMeta = tagMetas.get(i);
            TagValue tagValue = new TagValue();
            tagValue.setName(tagMeta.getName());
            tagValue.setValue(DataGenerator.randomValue(tagMeta.getType()));
            tagValues.add(tagValue);
        }
        return tagValues;
    }
}
