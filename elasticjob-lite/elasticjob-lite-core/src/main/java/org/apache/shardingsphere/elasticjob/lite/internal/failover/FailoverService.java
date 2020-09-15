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

package org.apache.shardingsphere.elasticjob.lite.internal.failover;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobScheduleController;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingNode;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingService;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Failover service.
 * 作业失效转移服务
 */
@Slf4j
public final class FailoverService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final ShardingService shardingService;
    
    public FailoverService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
    }
    
    /**
     * set crashed failover flag.
     * 
     * @param item crashed job item
     */
    public void setCrashedFailoverFlag(final int item) {
        if (!isFailoverAssigned(item)) {
            jobNodeStorage.createJobNodeIfNeeded(FailoverNode.getItemsNode(item));
        }
    }
    
    private boolean isFailoverAssigned(final Integer item) {
        return jobNodeStorage.isJobNodeExisted(FailoverNode.getExecutionFailoverNode(item));
    }
    
    /**
     * Failover if necessary.
     */
    public void failoverIfNecessary() {
        if (needFailover()) {
            jobNodeStorage.executeInLeader(FailoverNode.LATCH, new FailoverLeaderExecutionCallback());
        }
    }

    /**
     * 需要失败转移
     * 条件一：${JOB_NAME}/leader/failover/items/${ITEM_ID} 有失效转移的作业分片项。
     * 条件二：当前作业不在运行中。此条件即是上文提交的作业节点空闲的定义。
     *
     * 失效转移： 运行中的作业服务器崩溃不会导致重新分片，只会在下次作业启动时分片。启用失效转移功能可以在本次作业执行过程中，监测其他作业服务器【空闲】，抓取未完成的孤儿分片项执行
     * 调用 JobNodeStorage#executeInLeader(…) 方法，使用 FailoverNode.LATCH( /${JOB_NAME}/leader/failover/latch ) 路径构成的分布式锁，保证 FailoverLeaderExecutionCallback 的回调方法同一时间，即使多个作业节点调用，有且仅有一个作业节点进行执行。
     * 另外，虽然 JobNodeStorage#executeInLeader(…) 方法上带有 Leader 关键字，实际非必须在主节点的操作，任何一个拿到分布式锁的作业节点都可以调用。目前和分布式锁相关的逻辑，在 Elastic-Job-Lite 里，都会调用 JobNodeStorage#executeInLeader(…) 方法，数据都存储在 /leader/ 节点目录下。
     * @return
     */
    private boolean needFailover() {
        return jobNodeStorage.isJobNodeExisted(FailoverNode.ITEMS_ROOT) && !jobNodeStorage.getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT).isEmpty()
                && !JobRegistry.getInstance().isJobRunning(jobName);
    }
    
    /**
     * Update sharding items status when failover execution complete.
     * 
     * @param items sharding items of failover execution completed
     */
    public void updateFailoverComplete(final Collection<Integer> items) {
        for (int each : items) {
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getExecutionFailoverNode(each));
        }
    }
    
    /**
     * Get failover items.
     * 获得关闭作业节点( ${JOB_INSTANCE_ID} )对应的 ${JOB_NAME}/sharding/${ITEM_ID}/failover 作业分片项。
     * @param jobInstanceId job instance ID
     * @return failover items
     */
    public List<Integer> getFailoverItems(final String jobInstanceId) {
        List<String> items = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT);
        List<Integer> result = new ArrayList<>(items.size());
        for (String each : items) {
            int item = Integer.parseInt(each);
            String node = FailoverNode.getExecutionFailoverNode(item);
            //${JOB_NAME}/sharding/${ITEM_ID}/failover
            if (jobNodeStorage.isJobNodeExisted(node) && jobInstanceId.equals(jobNodeStorage.getJobNodeDataDirectly(node))) {
                result.add(item);
            }
        }
        Collections.sort(result);
        return result;
    }
    
    /**
     * Get failover items which execute on localhost.
     * 
     * @return failover items which execute on localhost
     */
    public List<Integer> getLocalFailoverItems() {
        if (JobRegistry.getInstance().isShutdown(jobName)) {
            return Collections.emptyList();
        }
        return getFailoverItems(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
    
    /**
     * 获取运行在本作业服务器的被失效转移的序列号.
     * 
     * @return 运行在本作业服务器的被失效转移的序列号
     */
    public List<Integer> getLocalTakeOffItems() {
        List<Integer> shardingItems = shardingService.getLocalShardingItems();
        List<Integer> result = new ArrayList<>(shardingItems.size());
        for (int each : shardingItems) {
            if (jobNodeStorage.isJobNodeExisted(FailoverNode.getExecutionFailoverNode(each))) {
                result.add(each);
            }
        }
        return result;
    }
    
    /**
     * Remove failover info.
     */
    public void removeFailoverInfo() {
        for (String each : jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT)) {
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getExecutionFailoverNode(Integer.parseInt(each)));
        }
    }



    /**
     * 回调
     *
     *
     * 再次调用 #needFailover() 方法，确保经过分布式锁获取等待过程中，仍然需要失效转移。因为可能多个作业节点调用了该回调，第一个作业节点执行了失效转移，可能第二个作业节点就不需要执行失效转移了。
     * 调用 JobNodeStorage#getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT)#get(0) 方法，获得一个 ${JOB_NAME}/leader/failover/items/${ITEM_ID} 作业分片项。
     * 调用 JobNodeStorage#fillEphemeralJobNode(...) 方法，设置这个临时数据节点 ${JOB_NAME}/sharding/${ITEM_ID}failover 作业分片项为当前作业节点( ${JOB_INSTANCE_ID} )。
     * 调用 JobNodeStorage#removeJobNodeIfExisted(...) 方法，移除这个${JOB_NAME}/leader/failover/items/${ITEM_ID} 作业分片项。
     * 调用 JobScheduleController#triggerJob() 方法，立即启动作业。调用该方法，实际作业不会立即执行，而仅仅是进行触发。
     *              如果有多个失效转移的作业分片项，多次调用 JobScheduleController#triggerJob() 方法会不会导致作业是并行执行的？答案是不会，因为一个作业的 Quartz 线程数设置为 1。
     */
    class FailoverLeaderExecutionCallback implements LeaderExecutionCallback {
        
        @Override
        public void execute() {
            // 判断需要失效转移
            if (JobRegistry.getInstance().isShutdown(jobName) || !needFailover()) {
                return;
            }
            // 获得一个 `${JOB_NAME}/leader/failover/items/${ITEM_ID}` 作业分片项
            int crashedItem = Integer.parseInt(jobNodeStorage.getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT).get(0));
            log.debug("Failover job '{}' begin, crashed item '{}'", jobName, crashedItem);
            // 设置这个 `${JOB_NAME}/sharding/${ITEM_ID}/failover` 作业分片项 为 当前作业节点
            jobNodeStorage.fillEphemeralJobNode(FailoverNode.getExecutionFailoverNode(crashedItem), JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
            // 移除这个 `${JOB_NAME}/leader/failover/items/${ITEM_ID}` 作业分片项
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getItemsNode(crashedItem));
            // TODO Instead of using triggerJob, use executor for unified scheduling
            // 触发作业执行
            JobScheduleController jobScheduleController = JobRegistry.getInstance().getJobScheduleController(jobName);
            if (null != jobScheduleController) {
                jobScheduleController.triggerJob();
            }
        }
    }
}
