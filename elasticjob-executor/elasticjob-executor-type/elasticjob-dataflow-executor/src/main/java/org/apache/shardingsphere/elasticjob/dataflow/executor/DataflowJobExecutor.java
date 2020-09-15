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

package org.apache.shardingsphere.elasticjob.dataflow.executor;

import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.dataflow.job.DataflowJob;
import org.apache.shardingsphere.elasticjob.dataflow.props.DataflowJobProperties;
import org.apache.shardingsphere.elasticjob.executor.JobFacade;
import org.apache.shardingsphere.elasticjob.executor.item.impl.ClassedJobItemExecutor;

import java.util.List;

/**
 * Dataflow job executor.
 * 数据流式执行器
 * 支持两种模式
 *    一次性的数据流（连续的特殊形势）
 *    连续的数据流处理
 */
public final class DataflowJobExecutor implements ClassedJobItemExecutor<DataflowJob> {
    
    @Override
    public void process(final DataflowJob elasticJob, final JobConfiguration jobConfig, final JobFacade jobFacade, final ShardingContext shardingContext) {
        if (Boolean.parseBoolean(jobConfig.getProps().getOrDefault(DataflowJobProperties.STREAM_PROCESS_KEY, false).toString())) {
            streamingExecute(elasticJob, jobConfig, jobFacade, shardingContext);
        } else {
            oneOffExecute(elasticJob, shardingContext);
        }
    }

    /**
     * 流式处理
     * 不断加载数据，不断处理数据，直到数据为空 或者 作业不适合继续运行：
     * 如果采用流式作业处理方式，建议processData处理数据后更新其状态，避免fetchData再次抓取到，从而使得作业永不停止。 流式数据处理参照TbSchedule设计，适用于不间歇的数据处理。
     * 作业需要重新分片，所以不适合继续流式数据处理。
     * @param shardingContext 分片上下文
     */
    private void streamingExecute(final DataflowJob elasticJob, final JobConfiguration jobConfig, final JobFacade jobFacade, final ShardingContext shardingContext) {
        List<Object> data = fetchData(elasticJob, shardingContext);
        while (null != data && !data.isEmpty()) {
            processData(elasticJob, shardingContext, data);
            if (!isEligibleForJobRunning(jobConfig, jobFacade)) {
                break;
            }
            data = fetchData(elasticJob, shardingContext);
        }
    }
    
    private boolean isEligibleForJobRunning(final JobConfiguration jobConfig, final JobFacade jobFacade) {
        return !jobFacade.isNeedSharding() && Boolean.parseBoolean(jobConfig.getProps().getOrDefault(DataflowJobProperties.STREAM_PROCESS_KEY, false).toString());
    }

    /**
     * 一次处理
     *
     * @param shardingContext 分片上下文
     */
    private void oneOffExecute(final DataflowJob elasticJob, final ShardingContext shardingContext) {
        List<Object> data = fetchData(elasticJob, shardingContext);
        if (null != data && !data.isEmpty()) {
            processData(elasticJob, shardingContext, data);
        }
    }

    /**
     * 加载数据
     *
     * @param shardingContext 分片上下文
     * @return 数据
     */
    @SuppressWarnings("unchecked")
    private List<Object> fetchData(final DataflowJob elasticJob, final ShardingContext shardingContext) {
        return elasticJob.fetchData(shardingContext);
    }

    /**
     * 处理数据
     *
     * @param shardingContext 分片上下文
     * @param data 数据
     */
    @SuppressWarnings("unchecked")
    private void processData(final DataflowJob elasticJob, final ShardingContext shardingContext, final List<Object> data) {
        elasticJob.processData(shardingContext, data);
    }
    
    @Override
    public Class<DataflowJob> getElasticJobClass() {
        return DataflowJob.class;
    }
}
