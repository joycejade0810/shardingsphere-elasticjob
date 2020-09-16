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

package org.apache.shardingsphere.elasticjob.lite.internal.reconcile;

import com.google.common.util.concurrent.AbstractScheduledService;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.election.LeaderService;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingService;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.concurrent.TimeUnit;

/**
 * Reconcile service.
 * 调解分布式作业不一致状态服务
 *
 *
 * 调解分布式作业不一致状态服务一共有三个条件：
     * 调用 LeaderService#isLeaderUntilBlock() 方法，判断当前作业节点是否为主节点。在《Elastic-Job-Lite 源码分析 —— 主节点选举》有详细解析。
     * 调用 ShardingService#isNeedSharding() 方法，判断当前作业是否需要重分片。如果需要重新分片，就不要重复设置当前作业需要重新分片的标记。
     * 调用 ShardingService#hasShardingInfoInOfflineServers() 方法，查询是否包含有分片节点的不在线服务器。
 *                 永久数据节点 /${JOB_NAME}/sharding/${ITEM_INDEX}/instance存储分配的作业节点主键( ${JOB_INSTANCE_ID} )，
 *                 不会随着作业节点因为各种原因断开后会话超时移除，而临时数据节点/${JOB_NAME}/instances/${JOB_INSTANCE_ID} 会随着作业节点因为各种原因断开后超时会话超时移除。
 *                 当查询到包含有分片节点的不在线的作业节点，设置需要重新分片的标记后进行重新分片，将其持有的作业分片分配给其它在线的作业节点。
 */
@Slf4j
public final class ReconcileService extends AbstractScheduledService {
    
    private long lastReconcileTime;
    
    private final ConfigurationService configService;
    
    private final ShardingService shardingService;
    
    private final LeaderService leaderService;
    
    public ReconcileService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        lastReconcileTime = System.currentTimeMillis();
        configService = new ConfigurationService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
    }

    /**
     * 每 1 分钟会调用一次 #runOneIteration() 方法进行校验。
     * 达到周期性校验注册中心数据与真实作业状态的一致性。
     */
    @Override
    protected void runOneIteration() {
        int reconcileIntervalMinutes = configService.load(true).getReconcileIntervalMinutes();
        if (reconcileIntervalMinutes > 0 && (System.currentTimeMillis() - lastReconcileTime >= reconcileIntervalMinutes * 60 * 1000)) {
            lastReconcileTime = System.currentTimeMillis();
            if (leaderService.isLeaderUntilBlock() && !shardingService.isNeedSharding() && shardingService.hasShardingInfoInOfflineServers()) {
                log.warn("Elastic Job: job status node has inconsistent value,start reconciling...");
                shardingService.setReshardingFlag();
            }
        }
    }
    
    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, 1, TimeUnit.MINUTES);
    }
}
