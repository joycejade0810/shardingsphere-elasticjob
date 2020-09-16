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

package org.apache.shardingsphere.elasticjob.api.listener;

/**
 * ElasticJob listener.
 * 作业监听器接口,每台作业节点均执行
 * 若作业处理作业服务器的文件，处理完成后删除文件，可考虑使用每个节点均执行清理任务。此类型任务实现简单，且无需考虑全局分布式任务是否完成，请尽量使用此类型监听器。
 */
public interface ElasticJobListener {

    /**
     * 作业执行前的执行的方法.
     *
     * @param shardingContexts 分片上下文
     */
    void beforeJobExecuted(ShardingContexts shardingContexts);

    /**
     * 作业执行后的执行的方法.
     *
     * @param shardingContexts 分片上下文
     */
    void afterJobExecuted(ShardingContexts shardingContexts);
}
