package com.taosdata.tsync.repository;

import com.taosdata.tsync.entity.RunnableTask;

import java.util.ArrayList;
import java.util.List;

public final class RunnableTaskRepository {

    private List<RunnableTask> taskList = new ArrayList<>();
    private static volatile RunnableTaskRepository instance;
    private static final int NULL = -1;

    private RunnableTaskRepository() {
    }

    public static RunnableTaskRepository getInstance() {
        if (instance == null) {
            synchronized (RunnableTaskRepository.class) {
                if (instance == null)
                    instance = new RunnableTaskRepository();
            }
        }
        return instance;
    }

    public RunnableTask find(int taskId) {
        for (RunnableTask task : taskList) {
            if (taskId == task.getId()) {
                return task;
            }
        }
        return null;
    }

    public void add(RunnableTask runnableTask) {
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
        for (int index = 0; index < taskList.size(); index++) {
            if (id == taskList.get(index).getId())
                return index;
        }
        return NULL;
    }
}