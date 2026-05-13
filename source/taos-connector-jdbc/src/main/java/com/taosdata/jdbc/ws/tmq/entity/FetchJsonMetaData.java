package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.ws.tmq.meta.Meta;

import java.util.List;

public class FetchJsonMetaData {
    @JsonProperty("tmq_meta_version")
    private String tmqMetaVersion;
    @JsonProperty("metas")
    private List<Meta> metas;

    public String getTmqMetaVersion() {
        return tmqMetaVersion;
    }

    public void setTmqMetaVersion(String tmqMetaVersion) {
        this.tmqMetaVersion = tmqMetaVersion;
    }

    public List<Meta> getMetas() {
        return metas;
    }

    public void setMetas(List<Meta> metas) {
        this.metas = metas;
    }

}
