package com.taosdata.taosx.pspace;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

@Data
public class CheckResult {
    private boolean valid;
    private boolean support;
    @SerializedName("data_source")
    private String dataSource;
    private String version;
    private String message;

    public static CheckResult valid(String version) {
        CheckResult res = new CheckResult();
        res.setValid(true);
        res.setSupport(true);
        res.setDataSource("pspace");
        res.setVersion(version);
        return res;
    }

    public static CheckResult invalid(String msg) {
        CheckResult res = new CheckResult();
        res.setValid(false);
        res.setDataSource("pspace");
        res.setMessage(msg);
        return res;
    }
}
