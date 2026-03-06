package com.taosdata.taosx.pspace.config;

import com.google.gson.annotations.SerializedName;
import com.sunwayland.pspace.PSpaceClient;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@ToString(exclude = "password")
public class Connection {
    private String server;
    private Integer port;
    private String username;
    private String password;

    @SerializedName("timeout_sec")
    private long timeoutSec = 30;

    /**
     * Return a masked password for safe display.
     */
    public String getMaskedPassword() {
        if (password == null)
            return null;
        if (password.length() <= 2)
            return "**";
        return password.charAt(0) + "******" + password.charAt(password.length() - 1);
    }

    public PSpaceClient toPSpaceClient() {
        return PSpaceClient.getInstance(server, port, username, password);
    }
}
