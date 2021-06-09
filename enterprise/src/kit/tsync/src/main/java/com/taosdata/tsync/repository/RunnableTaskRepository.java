package com.taosdata.tsync.repository;

import com.taosdata.tsync.entity.RunnableTask;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

    public RunnableTask find(UUID taskId) {
        for (RunnableTask task : taskList) {
            if (taskId.equals(task.getId())) {
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

    public void delete(UUID taskId) {
        int index = findIndex(taskId);
        if (index != NULL)
            taskList.remove(index);
    }

    private int findIndex(UUID id) {
        for (int i = 0; i < taskList.size(); i++) {
            if (id.equals(taskList.get(i)))
                return i;
        }
        return NULL;
    }
}