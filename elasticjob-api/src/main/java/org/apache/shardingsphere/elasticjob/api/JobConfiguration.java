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

package org.apache.shardingsphere.elasticjob.api;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Properties;

/**
 * ElasticJob configuration.
 * job 配置类
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class JobConfiguration {

    //作业名称
    private final String jobName;

    //cron表达式，用于控制作业触发时间
    private final String cron;

    //作业分片总数。如果一个作业启动超过作业分片总数的节点，只有 shardingTotalCount会执行作业。
    private final int shardingTotalCount;

    //分片序列号和参数 片序列号和参数用等号分隔，多个键值对用逗号分隔  **分片序列号从0开始，不可大于或等于作业分片总数 如： 0=a,1=b,2=c
    private final String shardingItemParameters;

    //作业自定义参数 可通过传递该参数为作业调度的业务方法传参，用于实现带参数的作业     //例：每次获取的数据量、作业实例从数据库读取的主键等
    private final String jobParameter;
    
    private final boolean monitorExecution;

    //是否开启作业执行失效转移。开启表示如果作业在一次作业执行中途宕机，允许将该次未完成的作业在另一作业节点上补偿执行
    private final boolean failover;

    //是否开启错过作业重新执行
    private final boolean misfire;
    
    private final int maxTimeDiffSeconds;
    
    private final int reconcileIntervalMinutes;
    
    private final String jobShardingStrategyType;
    
    private final String jobExecutorServiceHandlerType;
    
    private final String jobErrorHandlerType;

    //作业描述
    private final String description;
    
    private final Properties props;
    
    private final boolean disabled;
    
    private final boolean overwrite;
    
    /**
     * Create ElasticJob configuration builder.
     *
     * @param jobName job name
     * @param shardingTotalCount sharding total count
     * @return ElasticJob configuration builder
     */
    public static Builder newBuilder(final String jobName, final int shardingTotalCount) {
        return new Builder(jobName, shardingTotalCount);
    }
    
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        
        private final String jobName;
        
        private String cron;
        
        private final int shardingTotalCount;
        
        private String shardingItemParameters = "";
        
        private String jobParameter = "";

        //监控作业运行时状态
        private boolean monitorExecution = true;
        
        private boolean failover;
        
        private boolean misfire = true;

        //设置最大容忍的本机与注册中心的时间误差秒数
        private int maxTimeDiffSeconds = -1;

        //修复作业服务器不一致状态服务调度间隔时间，配置为小于1的任意值表示不执行修复
        private int reconcileIntervalMinutes = 10;
        
        private String jobShardingStrategyType;
        
        private String jobExecutorServiceHandlerType;
        
        private String jobErrorHandlerType;
        
        private String description = "";
        
        private final Properties props = new Properties();

        //作业是否禁用执行
        private boolean disabled;

        //设置使用本地作业配置覆盖注册中心的作业配置
        private boolean overwrite;
    
        /**
         * Cron expression.
         *
         * @param cron cron expression
         * @return job configuration builder
         */
        public Builder cron(final String cron) {
            if (null != cron) {
                this.cron = cron;
            }
            return this;
        }
        
        /**
         * Set mapper of sharding items and sharding parameters.
         *
         * <p>
         * sharding item and sharding parameter split by =, multiple sharding items and sharding parameters split by comma, just like map.
         * Sharding item start from zero, cannot equal to great than sharding total count.
         *
         * For example:
         * 0=a,1=b,2=c
         * </p>
         *
         * @param shardingItemParameters mapper of sharding items and sharding parameters
         * @return job configuration builder
         */
        public Builder shardingItemParameters(final String shardingItemParameters) {
            if (null != shardingItemParameters) {
                this.shardingItemParameters = shardingItemParameters;
            }
            return this;
        }
        
        /**
         * Set job parameter.
         *
         * @param jobParameter job parameter
         *
         * @return job configuration builder
         */
        public Builder jobParameter(final String jobParameter) {
            if (null != jobParameter) {
                this.jobParameter = jobParameter;
            }
            return this;
        }
        
        /**
         * Set enable or disable monitor execution.
         *
         * <p>
         * For short interval job, it is better to disable monitor execution to improve performance. 
         * It can't guarantee repeated data fetch and can't failover if disable monitor execution, please keep idempotence in job.
         *
         * For long interval job, it is better to enable monitor execution to guarantee fetch data exactly once.
         * </p>
         *
         * @param monitorExecution monitor job execution status 
         * @return ElasticJob configuration builder
         */
        public Builder monitorExecution(final boolean monitorExecution) {
            this.monitorExecution = monitorExecution;
            return this;
        }
        
        /**
         * Set enable failover.
         *
         * <p>
         * Only for `monitorExecution` enabled.
         * </p> 
         *
         * @param failover enable or disable failover
         * @return job configuration builder
         */
        public Builder failover(final boolean failover) {
            this.failover = failover;
            return this;
        }
        
        /**
         * Set enable misfire.
         *
         * @param misfire enable or disable misfire
         * @return job configuration builder
         */
        public Builder misfire(final boolean misfire) {
            this.misfire = misfire;
            return this;
        }
        
        /**
         * Set max tolerate time different seconds between job server and registry center.
         *
         * <p>
         * ElasticJob will throw exception if exceed max tolerate time different seconds.
         * -1 means do not check.
         * </p>
         *
         * @param maxTimeDiffSeconds max tolerate time different seconds between job server and registry center
         * @return ElasticJob configuration builder
         */
        public Builder maxTimeDiffSeconds(final int maxTimeDiffSeconds) {
            this.maxTimeDiffSeconds = maxTimeDiffSeconds;
            return this;
        }
        
        /**
         * Set reconcile interval minutes for job sharding status.
         *
         * <p>
         * Monitor the status of the job server at regular intervals, and resharding if incorrect.
         * </p>
         *
         * @param reconcileIntervalMinutes reconcile interval minutes for job sharding status
         * @return ElasticJob configuration builder
         */
        public Builder reconcileIntervalMinutes(final int reconcileIntervalMinutes) {
            this.reconcileIntervalMinutes = reconcileIntervalMinutes;
            return this;
        }
        
        /**
         * Set job sharding sharding type.
         *
         * <p>
         * Default for {@code AverageAllocationJobShardingStrategy}.
         * </p>
         *
         * @param jobShardingStrategyType job sharding sharding type
         * @return ElasticJob configuration builder
         */
        public Builder jobShardingStrategyType(final String jobShardingStrategyType) {
            if (null != jobShardingStrategyType) {
                this.jobShardingStrategyType = jobShardingStrategyType;
            }
            return this;
        }
        
        /**
         * Set job executor service handler type.
         *
         * @param jobExecutorServiceHandlerType job executor service handler type
         * @return job configuration builder
         */
        public Builder jobExecutorServiceHandlerType(final String jobExecutorServiceHandlerType) {
            this.jobExecutorServiceHandlerType = jobExecutorServiceHandlerType;
            return this;
        }
        
        /**
         * Set job error handler type.
         *
         * @param jobErrorHandlerType job error handler type
         * @return job configuration builder
         */
        public Builder jobErrorHandlerType(final String jobErrorHandlerType) {
            this.jobErrorHandlerType = jobErrorHandlerType;
            return this;
        }
        
        /**
         * Set job description.
         *
         * @param description job description
         * @return job configuration builder
         */
        public Builder description(final String description) {
            if (null != description) {
                this.description = description;
            }
            return this;
        }
        
        /**
         * Set property.
         *
         * @param key property key
         * @param value property value
         * @return job configuration builder
         */
        public Builder setProperty(final String key, final String value) {
            props.setProperty(key, value);
            return this;
        }
        
        /**
         * Set whether disable job when start.
         * 
         * <p>
         * Using in job deploy, start job together after deploy.
         * </p>
         *
         * @param disabled whether disable job when start
         * @return ElasticJob configuration builder
         */
        public Builder disabled(final boolean disabled) {
            this.disabled = disabled;
            return this;
        }
        
        /**
         * Set whether overwrite local configuration to registry center when job startup. 
         * 
         * <p>
         *  If overwrite enabled, every startup will use local configuration.
         * </p>
         *
         * @param overwrite whether overwrite local configuration to registry center when job startup
         * @return ElasticJob configuration builder
         */
        public Builder overwrite(final boolean overwrite) {
            this.overwrite = overwrite;
            return this;
        }
        
        /**
         * Build ElasticJob configuration.
         *
         * 生成器模式
         * @return ElasticJob configuration
         */
        public final JobConfiguration build() {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(jobName), "jobName can not be empty.");
            Preconditions.checkArgument(shardingTotalCount > 0, "shardingTotalCount should larger than zero.");
            return new JobConfiguration(jobName, cron, shardingTotalCount, shardingItemParameters, jobParameter, 
                    monitorExecution, failover, misfire, maxTimeDiffSeconds, reconcileIntervalMinutes,
                    jobShardingStrategyType, jobExecutorServiceHandlerType, jobErrorHandlerType, description, props, disabled, overwrite);
        }
    }
}
