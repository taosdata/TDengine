package com.taosdata.tsync.entity;

import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Job {
    private static final Logger logger = LoggerFactory.getLogger(Job.class);

    private final UUID id;
    private final ConfigurationType configurationType;
    private final UUID configurationId;
    private JobStatus status;
    private List<UUID> taskIds = new ArrayList<>();

    public Job(ConfigurationType configurationType, UUID configurationId) {
        this.id = UUID.randomUUID();
        this.configurationType = configurationType;
        this.configurationId = configurationId;
        this.status = JobStatus.INIT;
    }

    public void prepare(JobService jobExecuteService) {
        try {
            this.taskIds = jobExecuteService.prepare(configurationType, configurationId);
        } catch (Exception e) {
            this.status = JobStatus.EXCEPTION;
            e.printStackTrace();
            logger.error(e.getMessage());
            return;
        }
        this.status = JobStatus.PREPARED;
    }

    public void execute(JobService jobExecuteService) {
        if (this.status != JobStatus.PREPARED) {
            logger.error(">>> job status is not prepared");
            return;
        }

        logger.info(">>> start to execute job: " + id.toString() + "");
        this.status = JobStatus.RUNNING;
        // start all tasks and wait them to be finished
        try {
            jobExecuteService.startAndWait(taskIds);
        } catch (Exception e) {
            this.status = JobStatus.EXCEPTION;
            logger.error(e.getMessage());
            logger.error("exception happened during execute Job: " + id.toString());
        }
        this.status = JobStatus.COMPLETED;
        logger.info(">>> job: " + id.toString() + "is done.");
    }

    public Configuration getConfiguration(JobService jobService) {
        return jobService.getConfiguration(this.configurationType, this.configurationId);
    }

    public JobStatus getStatus() {
        return this.status;
    }

}
