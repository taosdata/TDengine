package com.taosdata.taosdemo.service;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AbstractService {

    protected int getAffectRows(List<Future<Integer>> futureList) {
        int count = 0;
        for (Future<Integer> future : futureList) {
            try {
                count += future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    protected int getAffectRows(Future<Integer> future) {
        int count = 0;
        try {
            count += future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return count;
    }

}
