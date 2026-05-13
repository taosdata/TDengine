package com.taosdata.tsync.service;

public interface ResultProcessService {

    void process(Object result);

    Object getResult();
}