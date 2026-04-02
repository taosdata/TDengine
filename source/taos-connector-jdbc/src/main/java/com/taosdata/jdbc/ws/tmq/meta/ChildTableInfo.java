package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;

public class ChildTableInfo extends Meta {
    private String using;
    private int tagNum;
    private List<Tag> tags;

    public String getUsing() {
        return using;
    }

    public void setUsing(String using) {
        this.using = using;
    }

    public int getTagNum() {
        return tagNum;
    }

    public void setTagNum(int tagNum) {
        this.tagNum = tagNum;
    }

    public List<Tag> getTags() {
        return tags;
    }

    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }
}