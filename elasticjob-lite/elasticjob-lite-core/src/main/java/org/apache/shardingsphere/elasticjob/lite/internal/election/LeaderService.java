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

package org.apache.shardingsphere.elasticjob.lite.internal.election;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.infra.concurrent.BlockUtils;

/**
 * Leader service.
 *  选举主节点
 */
@Slf4j
public final class LeaderService {
    
    private final String jobName;
    
    private final ServerService serverService;
    
    private final JobNodeStorage jobNodeStorage;
    
    public LeaderService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
    }
    
    /**
     * Elect leader.
     */
    public void electLeader() {
        log.debug("Elect a new leader now.");
        jobNodeStorage.executeInLeader(LeaderNode.LATCH, new LeaderElectionExecutionCallback());
        log.debug("Leader election completed.");
    }

    /**
     * 判断当前节点是否是主节点.
     *
     * 如果主节点正在选举中而导致取不到主节点, 则阻塞至主节点选举完成再返回.
     *
     * @return 当前节点是否是主节点
     */
    public boolean isLeaderUntilBlock() {
        // 不存在主节点 && 有可用的服务器节点
        while (!hasLeader() && serverService.hasAvailableServers()) {
            log.info("Leader is electing, waiting for {} ms", 100);
            //选举不到主节点进行等待，避免不间断、无间隔的进行主节点选举。
            BlockUtils.waitingShortTime();
            if (!JobRegistry.getInstance().isShutdown(jobName) && serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp())) {
                electLeader();
            }
        }
        // 不存在主节点 && 有可用的服务器节点
        return isLeader();
    }
    
    /**
     * Judge current server is leader or not.
     *
     * @return current server is leader or not
     */
    public boolean isLeader() {
        return !JobRegistry.getInstance().isShutdown(jobName) && JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId().equals(jobNodeStorage.getJobNodeData(LeaderNode.INSTANCE));
    }

    /**
     * 判断是否已经有主节点.
     *
     * @return 是否已经有主节点
     */
    public boolean hasLeader() {
        return jobNodeStorage.isJobNodeExisted(LeaderNode.INSTANCE);
    }
    
    /**
     * 删除主节点供重新选举
     *  删除主节点时机
         * 第一种，主节点进程正常关闭时。JobShutdownHookPlugin #shutdown()
         * 第二种，主节点进程 CRASHED 时。
         * 第三种，作业被禁用时。LeaderAbdicationJobListener #dataChanged()
         * 第四种，主节点进程远程关闭。 InstanceShutdownStatusJobListener #dataChanged()
     */
    public void removeLeader() {
        jobNodeStorage.removeJobNodeIfExisted(LeaderNode.INSTANCE);
    }
    
    @RequiredArgsConstructor
    class LeaderElectionExecutionCallback implements LeaderExecutionCallback {
        
        @Override
        public void execute() {
            if (!hasLeader()) {
                jobNodeStorage.fillEphemeralJobNode(LeaderNode.INSTANCE, JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
            }
        }
    }
}
