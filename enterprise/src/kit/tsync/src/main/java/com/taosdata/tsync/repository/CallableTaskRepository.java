package com.taosdata.tsync.repository;

import com.taosdata.tsync.entity.CallableTask;
import com.taosdata.tsync.entity.RunnableTask;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class CallableTaskRepository {

    private List<CallableTask> taskList = new ArrayList<>();
    private static volatile CallableTaskRepository instance;
    private static final int NULL = -1;

    private CallableTaskRepository() {
    }

    public static CallableTaskRepository getInstance() {
        if (instance == null) {
            synchronized (CallableTaskRepository.class) {
                if (instance == null)
                    instance = new CallableTaskRepository();
            }
        }
        return instance;
    }

    public CallableTask find(int taskId) {
        for (CallableTask task : taskList) {
            if (taskId == task.getId()) {
                return task;
            }
        }
        return null;
    }

    public void add(CallableTask runnableTask) {
        int index = findIndex(runnableTask.getId());
        if (index == NULL)
            taskList.add(runnableTask);
    }

    public void delete(int taskId) {
        int index = findIndex(taskId);
        if (index != NULL)
            taskList.remove(index);
    }

    private int findIndex(int id) {
        for (int i = 0; i < taskList.size(); i++) {
            if (id == taskList.get(i).getId())
                return i;
        }
        return NULL;
    }
}