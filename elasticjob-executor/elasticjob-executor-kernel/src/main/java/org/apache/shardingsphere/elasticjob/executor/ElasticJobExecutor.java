/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.elasticjob.executor;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.listener.ShardingContexts;
import org.apache.shardingsphere.elasticjob.infra.env.IpUtils;
import org.apache.shardingsphere.elasticjob.infra.exception.ExceptionUtils;
import org.apache.shardingsphere.elasticjob.infra.exception.JobExecutionEnvironmentException;
import org.apache.shardingsphere.elasticjob.infra.handler.error.JobErrorHandler;
import org.apache.shardingsphere.elasticjob.infra.handler.error.JobErrorHandlerFactory;
import org.apache.shardingsphere.elasticjob.infra.handler.threadpool.JobExecutorServiceHandlerFactory;
import org.apache.shardingsphere.elasticjob.executor.item.JobItemExecutor;
import org.apache.shardingsphere.elasticjob.executor.item.JobItemExecutorFactory;
import org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent;
import org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent.ExecutionSource;
import org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * ElasticJob executor.
 * 执行器
 */
@Slf4j
public final class ElasticJobExecutor {
    
    private final ElasticJob elasticJob;
    
    private final JobConfiguration jobConfig;
    
    private final JobFacade jobFacade;
    
    private final JobItemExecutor jobItemExecutor;
    
    private final ExecutorService executorService;
    
    private final JobErrorHandler jobErrorHandler;
    
    private final Map<Integer, String> itemErrorMessages;
    
    public ElasticJobExecutor(final ElasticJob elasticJob, final JobConfiguration jobConfig, final JobFacade jobFacade) {
        this(elasticJob, jobConfig, jobFacade, JobItemExecutorFactory.getExecutor(elasticJob.getClass()));
    }
    
    public ElasticJobExecutor(final String type, final JobConfiguration jobConfig, final JobFacade jobFacade) {
        this(null, jobConfig, jobFacade, JobItemExecutorFactory.getExecutor(type));
    }
    
    private ElasticJobExecutor(final ElasticJob elasticJob, final JobConfiguration jobConfig, final JobFacade jobFacade, final JobItemExecutor jobItemExecutor) {
        this.elasticJob = elasticJob;
        this.jobConfig = jobConfig;
        this.jobFacade = jobFacade;
        this.jobItemExecutor = jobItemExecutor;
        executorService = JobExecutorServiceHandlerFactory.getHandler(jobConfig.getJobExecutorServiceHandlerType()).createExecutorService(jobConfig.getJobName());
        jobErrorHandler = JobErrorHandlerFactory.getHandler(jobConfig.getJobErrorHandlerType());
        itemErrorMessages = new ConcurrentHashMap<>(jobConfig.getShardingTotalCount(), 1);
    }
    
    /**
     * Execute job.
     */
    public void execute() {
        try {
            //todo 1.检查环境配置 方法校验本机时间是否合法
            jobFacade.checkJobExecutionEnvironment();
        } catch (final JobExecutionEnvironmentException cause) {
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
        //todo 2.获取 当前作业服务器的分片上下文
        ShardingContexts shardingContexts = jobFacade.getShardingContexts();
        //todo 3.发布作业状态追踪事件
        jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_STAGING, String.format("Job '%s' execute begin.", jobConfig.getJobName()));
        //todo 4.跳过 存在运行中的被错过作业
        if (jobFacade.misfireIfRunning(shardingContexts.getShardingItemParameters().keySet())) {
            jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format(
                    "Previous job '%s' - shardingItems '%s' is still running, misfired job will start after previous job completed.", jobConfig.getJobName(),
                    shardingContexts.getShardingItemParameters().keySet()));
            return;
        }
        try {
            //todo 5.执行job前置监听
            jobFacade.beforeJobExecuted(shardingContexts);
            //CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            //CHECKSTYLE:ON
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
        //todo 6.执行job
        execute(shardingContexts, ExecutionSource.NORMAL_TRIGGER);
        // 执行 被跳过触发的作业
        while (jobFacade.isExecuteMisfired(shardingContexts.getShardingItemParameters().keySet())) {
            jobFacade.clearMisfire(shardingContexts.getShardingItemParameters().keySet());
            execute(shardingContexts, ExecutionSource.MISFIRE);
        }
        //todo 7.判断job是否需要失败转移
        jobFacade.failoverIfNecessary();
        try {
            //todo 8.执行job后置监听器
            jobFacade.afterJobExecuted(shardingContexts);
            //CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            //CHECKSTYLE:ON
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
    }

    /**
     * 执行多个作业的分片
     *
     * @param shardingContexts 分片上下文集合
     * @param executionSource 执行来源
     */
    private void execute(final ShardingContexts shardingContexts, final ExecutionSource executionSource) {
        if (shardingContexts.getShardingItemParameters().isEmpty()) {
            jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format("Sharding item for job '%s' is empty.", jobConfig.getJobName()));
            return;
        }
        jobFacade.registerJobBegin(shardingContexts);
        String taskId = shardingContexts.getTaskId();
        jobFacade.postJobStatusTraceEvent(taskId, State.TASK_RUNNING, "");
        try {
            process(shardingContexts, executionSource);
        } finally {
            // TODO Consider increasing the status of job failure, and how to handle the overall loop of job failure
            jobFacade.registerJobCompleted(shardingContexts);
            if (itemErrorMessages.isEmpty()) {
                jobFacade.postJobStatusTraceEvent(taskId, State.TASK_FINISHED, "");
            } else {
                jobFacade.postJobStatusTraceEvent(taskId, State.TASK_ERROR, itemErrorMessages.toString());
            }
        }
    }

    /**
     * 执行多个作业的分片
     *
     * @param shardingContexts 分片上下文集合
     * @param executionSource 执行来源
     *
     * 分配单分片项时，直接执行，无需使用线程池，性能更优。
     * 分配多分片项时，使用线程池并发执行，通过 CountDownLatch 实现等待分片项全部执行完成。
     */
    private void process(final ShardingContexts shardingContexts, final ExecutionSource executionSource) {
        Collection<Integer> items = shardingContexts.getShardingItemParameters().keySet();
        if (1 == items.size()) {//todo.只有一个分片，立即执行
            int item = shardingContexts.getShardingItemParameters().keySet().iterator().next();
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(IpUtils.getHostName(), IpUtils.getIp(), shardingContexts.getTaskId(), jobConfig.getJobName(), executionSource, item);
            process(shardingContexts, item, jobExecutionEvent);
            return;
        }

        //todo 多个分片并发执行，使用了CountDownLatch
        CountDownLatch latch = new CountDownLatch(items.size());
        for (int each : items) {
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(IpUtils.getHostName(), IpUtils.getIp(), shardingContexts.getTaskId(), jobConfig.getJobName(), executionSource, each);
            if (executorService.isShutdown()) {
                return;
            }
            executorService.submit(() -> {
                try {
                    // 执行一个作业
                    process(shardingContexts, each, jobExecutionEvent);
                } finally {
                    latch.countDown();
                }
            });
        }
        // 等待多分片全部完成
        try {
            latch.await();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 执行单个作业的分片
     *
     * @param shardingContexts 分片上下文集合
     * @param item 分片序号
     * @param startEvent 执行事件(开始)
     */
    @SuppressWarnings("unchecked")
    private void process(final ShardingContexts shardingContexts, final int item, final JobExecutionEvent startEvent) {
        // 发布执行事件(开始)
        jobFacade.postJobExecutionEvent(startEvent);
        log.trace("Job '{}' executing, item is: '{}'.", jobConfig.getJobName(), item);
        JobExecutionEvent completeEvent;
        try {
            // 执行单个作业
            jobItemExecutor.process(elasticJob, jobConfig, jobFacade, shardingContexts.createShardingContext(item));
            // 发布执行事件(成功)
            completeEvent = startEvent.executionSuccess();
            log.trace("Job '{}' executed, item is: '{}'.", jobConfig.getJobName(), item);
            jobFacade.postJobExecutionEvent(completeEvent);
            // CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            // CHECKSTYLE:ON

            // 发布执行事件(失败)
            completeEvent = startEvent.executionFailure(ExceptionUtils.transform(cause));
            jobFacade.postJobExecutionEvent(completeEvent);
            // 设置该分片执行异常信息
            itemErrorMessages.put(item, ExceptionUtils.transform(cause));
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
    }
    
    /**
     * Shutdown executor.
     */
    public void shutdown() {
        executorService.shutdown();
    }
}
