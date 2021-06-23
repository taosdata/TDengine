package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.CallableTask;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.repository.CallableTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractCallableJobService extends AbstractJobService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCallableJobService.class);

    protected final CallableTaskRepository callableTaskRepository = CallableTaskRepository.getInstance();
    private final ResultProcessService resultProcessService;

    protected String topic;
    protected int[] partitions;

    protected AbstractCallableJobService(ResultProcessService resultProcessService) {
        this.resultProcessService = resultProcessService;
    }

    @Override
    public abstract List<UUID> prepare(ConfigurationType configurationType, UUID jobConfigurationId) throws TsyncException;

    @Override
    public void startAndWait(List<UUID> taskIds) {
        List<FutureTask> futureTasks = new ArrayList<>();
        List<Thread> threads = IntStream.range(0, taskIds.size()).mapToObj(i -> {
            // each task create a thread
            UUID taskId = taskIds.get(i);
            CallableTask task = callableTaskRepository.find(taskId);
            FutureTask futureTask = new FutureTask<>(task.getCallable());
            futureTasks.add(futureTask);
            return new Thread(futureTask, "task-" + taskId);
        }).collect(Collectors.toList());
        // start
        threads.stream().forEach(Thread::start);
        // wait

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // get result
        for (FutureTask task : futureTasks) {
            Object result = null;
            try {
                result = task.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            resultProcessService.process(result);
        }
        Object result = resultProcessService.getResult();
        logger.info("get result: " + result.toString());
    }
}
