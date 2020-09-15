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

package org.apache.shardingsphere.elasticjob.infra.handler.sharding.impl;

import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobShardingStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Sharding strategy which for round robin by name job.
 * 根据作业名的哈希值对作业节点列表进行轮转的分片策略。
 *
 * AverageAllocationJobShardingStrategy的缺点是，一旦分片数小于作业作业节点数，作业将永远分配至IP地址靠前的作业节点，导致IP地址靠后的作业节点空闲。如：
 * OdevitySortByNameJobShardingStrategy则可以根据作业名称重新分配作业节点负载。
 * 如果有3台作业节点，分成2片，作业名称的哈希值为奇数，则每台作业节点分到的分片是：1=[0], 2=[1], 3=[]
 * 如果有3台作业节点，分成2片，作业名称的哈希值为偶数，则每台作业节点分到的分片是：3=[0], 2=[1], 1=[]
 */
public final class RoundRobinByNameJobShardingStrategy implements JobShardingStrategy {
    
    private final AverageAllocationJobShardingStrategy averageAllocationJobShardingStrategy = new AverageAllocationJobShardingStrategy();
    
    @Override
    public Map<JobInstance, List<Integer>> sharding(final List<JobInstance> jobInstances, final String jobName, final int shardingTotalCount) {
        return averageAllocationJobShardingStrategy.sharding(rotateServerList(jobInstances, jobName), jobName, shardingTotalCount);
    }
    
    private List<JobInstance> rotateServerList(final List<JobInstance> shardingUnits, final String jobName) {
        int shardingUnitsSize = shardingUnits.size();
        int offset = Math.abs(jobName.hashCode()) % shardingUnitsSize;
        if (0 == offset) {
            return shardingUnits;
        }
        List<JobInstance> result = new ArrayList<>(shardingUnitsSize);
        for (int i = 0; i < shardingUnitsSize; i++) {
            int index = (i + offset) % shardingUnitsSize;
            result.add(shardingUnits.get(index));
        }
        return result;
    }
    
    @Override
    public String getType() {
        return "ROUND_ROBIN";
    }
}
