package com.taosdata.taosx.pspace;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ServerInfo {
    private String serverName;
    private String version;
    private String userName;
    private String status;
}
