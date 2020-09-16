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

package org.apache.shardingsphere.elasticjob.lite.example.job.simple;

import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.lite.example.fixture.entity.Foo;
import org.apache.shardingsphere.elasticjob.lite.example.fixture.repository.FooRepository;
import org.apache.shardingsphere.elasticjob.lite.example.fixture.repository.FooRepositoryFactory;
import org.apache.shardingsphere.elasticjob.simple.job.SimpleJob;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class abc implements SimpleJob {
    
    private final FooRepository fooRepository = FooRepositoryFactory.getFooRepository();
    
    @Override
    public void execute(final ShardingContext shardingContext) {
        System.out.println(String.format("Item: %s | Time: %s | Thread: %s | %s",
                shardingContext.getShardingItem(), new SimpleDateFormat("HH:mm:ss").format(new Date()), Thread.currentThread().getId(), shardingContext.getJobName()));
        System.out.println("Thread="+Thread.currentThread().getId()+"==223123======分片参数="+shardingContext.getShardingParameter()+"========分片项="+shardingContext.getShardingItem()+"========分片总数="+shardingContext.getShardingTotalCount());
        List<Foo> data = fooRepository.findTodoData(shardingContext.getShardingParameter(), 10);
        for (Foo each : data) {
            System.out.println("业务层 传参数="+each.getLocation());
            fooRepository.setCompleted(each.getId());

        }
    }
}
