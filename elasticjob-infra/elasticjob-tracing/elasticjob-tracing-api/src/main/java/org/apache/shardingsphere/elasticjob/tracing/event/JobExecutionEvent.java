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

package org.apache.shardingsphere.elasticjob.tracing.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Date;
import java.util.UUID;

/**
 * Job execution event.
 * 作业执行追踪事件
 */
@RequiredArgsConstructor
@AllArgsConstructor
@Getter
public final class JobExecutionEvent implements JobEvent {
    /**
     * 主键
     */
    private String id = UUID.randomUUID().toString();
    /**
     * 主机名称
     */
    private final String hostname;
    /**
     * IP
     */
    private final String ip;
    /**
     * 作业任务ID
     */
    private final String taskId;
    /**
     * 作业名称
     */
    private final String jobName;
    /**
     * 任务来源
     */
    private final ExecutionSource source;
    /**
     * 作业分片项
     */
    private final int shardingItem;
    /**
     * 开始时间
     */
    private Date startTime = new Date();
    /**
     * 结束时间
     */
    @Setter
    private Date completeTime;
    /**
     * 是否执行成功
     */
    @Setter
    private boolean success;
    /**
     * 执行失败原因
     */
    @Setter
    private String failureCause;
    
    /**
     * Execution success.
     * 
     * @return job execution event
     */
    public JobExecutionEvent executionSuccess() {
        JobExecutionEvent result = new JobExecutionEvent(id, hostname, ip, taskId, jobName, source, shardingItem, startTime, completeTime, success, failureCause);
        result.setCompleteTime(new Date());
        result.setSuccess(true);
        return result;
    }
    
    /**
     * Execution failure.
     * 
     * @param failureCause failure cause
     * @return job execution event
     */
    public JobExecutionEvent executionFailure(final String failureCause) {
        JobExecutionEvent result = new JobExecutionEvent(id, hostname, ip, taskId, jobName, source, shardingItem, startTime, completeTime, success, failureCause);
        result.setCompleteTime(new Date());
        result.setSuccess(false);
        result.setFailureCause(failureCause);
        return result;
    }
    
    /**
     * Execution source.
     */
    public enum ExecutionSource {
        /**
         * 普通触发执行
         */
        NORMAL_TRIGGER,
        /**
         * 被错过执行
         */
        MISFIRE,
        /**
         * 失败转移后执行
         */
        FAILOVER
    }
}
