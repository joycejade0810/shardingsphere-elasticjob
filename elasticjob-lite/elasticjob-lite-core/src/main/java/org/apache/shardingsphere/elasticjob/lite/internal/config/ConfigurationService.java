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

package org.apache.shardingsphere.elasticjob.lite.internal.config;

import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.infra.exception.JobConfigurationException;
import org.apache.shardingsphere.elasticjob.infra.exception.JobExecutionEnvironmentException;
import org.apache.shardingsphere.elasticjob.infra.pojo.JobConfigurationPOJO;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.infra.env.TimeService;
import org.apache.shardingsphere.elasticjob.infra.yaml.YamlEngine;

/**
 * Configuration service.
 * 作业配置服务
 * 多个 Elastic-Job-Lite 使用相同注册中心和相同 namespace 组成集群，实现高可用。集群中，使用作业配置服务( ConfigurationService ) 共享作业配置。
 */
public final class ConfigurationService {

    //时间服务，提供当前时间查询
    private final TimeService timeService;

    //封装注册中心，提供存储服务
    private final JobNodeStorage jobNodeStorage;
    
    public ConfigurationService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        timeService = new TimeService();
    }
    
    /**
     * Load job configuration.
     * 读取作业配置.
     *
     * @param fromCache load from cache or not  是否从缓存中读取
     * @return job configuration  作业配置
     */
    public JobConfiguration load(final boolean fromCache) {
        String result;
        if (fromCache) {
            result = jobNodeStorage.getJobNodeData(ConfigurationNode.ROOT);
            if (null == result) {
                result = jobNodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT);
            }
        } else {
            result = jobNodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT);
        }
        return YamlEngine.unmarshal(result, JobConfigurationPOJO.class).toJobConfiguration();
    }
    
    /**
     * Set up job configuration.
     * 设置作业配置
     * 
     * @param jobClassName job class name
     * @param jobConfig job configuration to be updated
     * @return accepted job configuration
     */
    public JobConfiguration setUpJobConfiguration(final String jobClassName, final JobConfiguration jobConfig) {
        //校验注册中心存储的作业配置的作业实现类全路径( jobClass )和当前的是否相同，如果不同，则认为是冲突，不允许存储
        checkConflictJob(jobClassName, jobConfig);
        //当注册中心未存储该作业配置 或者 当前作业配置允许替换注册中心作业配置( overwrite = true )时，持久化作业配置。
        if (!jobNodeStorage.isJobNodeExisted(ConfigurationNode.ROOT) || jobConfig.isOverwrite()) {
            jobNodeStorage.replaceJobNode(ConfigurationNode.ROOT, YamlEngine.marshal(JobConfigurationPOJO.fromJobConfiguration(jobConfig)));
            jobNodeStorage.replaceJobRootNode(jobClassName);
            return jobConfig;
        }
        return load(false);
    }
    
    private void checkConflictJob(final String newJobClassName, final JobConfiguration jobConfig) {
        if (!jobNodeStorage.isJobRootNodeExisted()) {
            return;
        }
        String originalJobClassName = jobNodeStorage.getJobRootNodeData();
        if (null != originalJobClassName && !originalJobClassName.equals(newJobClassName)) {
            throw new JobConfigurationException(
                    "Job conflict with register center. The job '%s' in register center's class is '%s', your job class is '%s'", jobConfig.getJobName(), originalJobClassName, newJobClassName);
        }
    }
    
    /**
     * Check max time different seconds tolerable between job server and registry center.
     * 检查本机与注册中心的时间误差秒数是否在允许范围
     * todo Elastic-Job-Lite 作业触发是依赖本机时间，相同集群使用注册中心时间为基准
     *
     * @throws JobExecutionEnvironmentException throe JobExecutionEnvironmentException if exceed max time different seconds
     */
    public void checkMaxTimeDiffSecondsTolerable() throws JobExecutionEnvironmentException {
        int maxTimeDiffSeconds = load(true).getMaxTimeDiffSeconds();
        if (-1 == maxTimeDiffSeconds) {
            return;
        }
        long timeDiff = Math.abs(timeService.getCurrentMillis() - jobNodeStorage.getRegistryCenterTime());
        if (timeDiff > maxTimeDiffSeconds * 1000L) {
            throw new JobExecutionEnvironmentException(
                    "Time different between job server and register center exceed '%s' seconds, max time different is '%s' seconds.", timeDiff / 1000, maxTimeDiffSeconds);
        }
    }
}
