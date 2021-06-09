package com.taosdata.tsync;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

public class TsyncApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(TsyncApplication.class).web(WebApplicationType.NONE).run(args);
    }
}