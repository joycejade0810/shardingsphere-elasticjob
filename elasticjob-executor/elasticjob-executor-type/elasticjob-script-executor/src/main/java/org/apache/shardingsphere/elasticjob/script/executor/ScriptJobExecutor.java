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

package org.apache.shardingsphere.elasticjob.script.executor;

import com.google.common.base.Strings;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.infra.exception.JobConfigurationException;
import org.apache.shardingsphere.elasticjob.infra.exception.JobSystemException;
import org.apache.shardingsphere.elasticjob.executor.JobFacade;
import org.apache.shardingsphere.elasticjob.executor.item.impl.TypedJobItemExecutor;
import org.apache.shardingsphere.elasticjob.script.props.ScriptJobProperties;
import org.apache.shardingsphere.elasticjob.infra.json.GsonFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Script job executor.
 * 脚本作业执行器
 * Script类型作业意为脚本类型作业，支持shell，python，perl等所有类型脚本。
 * 只需通过控制台或代码配置scriptCommandLine即可，无需编码。执行脚本路径可包含参数，参数传递完毕后，作业框架会自动追加最后一个参数为作业运行时信息
 */
public final class ScriptJobExecutor implements TypedJobItemExecutor {

    /**
     * 执行脚本
     */
    @Override
    public void process(final ElasticJob elasticJob, final JobConfiguration jobConfig, final JobFacade jobFacade, final ShardingContext shardingContext) {
        CommandLine commandLine = CommandLine.parse(getScriptCommandLine(jobConfig.getProps()));
        // JSON 格式传递参数
        commandLine.addArgument(GsonFactory.getGson().toJson(shardingContext), false);
        try {
            new DefaultExecutor().execute(commandLine);
        } catch (final IOException ex) {
            throw new JobSystemException("Execute script failure.", ex);
        }
    }
    
    private String getScriptCommandLine(final Properties props) {
        String result = props.getProperty(ScriptJobProperties.SCRIPT_KEY);
        if (Strings.isNullOrEmpty(result)) {
            throw new JobConfigurationException("Cannot find script command line, job is not executed.");
        }
        return result;
    }
    
    @Override
    public String getType() {
        return "SCRIPT";
    }
}
