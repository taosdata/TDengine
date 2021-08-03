package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.repository.RunnableTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractRunnableJobService extends AbstractJobService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractRunnableJobService.class);

    protected final RunnableTaskRepository runnableTaskRepository = RunnableTaskRepository.getInstance();

    public abstract List<UUID> prepare(ConfigurationType configurationType, UUID jobConfigurationId) throws TsyncException;

    @Override
    public void startAndWait(List<UUID> taskIds) {
        // each task create a thread
        List<Thread> threads = IntStream.range(0, taskIds.size()).mapToObj(index -> {
            UUID taskId = taskIds.get(index);
            RunnableTask task = runnableTaskRepository.find(taskId);
            if (task == null) {
                String errorMsg = "cannot find task: " + taskId + " at runnable task repository";
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
            return new Thread(task.getRunnable(), "task-" + taskId);
        }).collect(Collectors.toList());

        // start
        threads.forEach(Thread::start);

        // wait
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
