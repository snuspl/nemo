/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.common.config;

import edu.snu.vortex.runtime.common.execplan.RuntimeAttributes;

/**
 * ExecutorConfig.
 */
public class ExecutorConfig {
  private final RtConfig.RtExecMode rtExecMode;
  private final RuntimeAttributes.ResourceType executorType;
  private final int numExecutionThreads;

  public ExecutorConfig(final RtConfig.RtExecMode rtExecMode,
                        final RuntimeAttributes.ResourceType executorType,
                        final int numExecutionThreads) {
    this.rtExecMode = rtExecMode;
    this.executorType = executorType;
    this.numExecutionThreads = numExecutionThreads;
  }

  public final int getNumExecutionThreads() {
    return numExecutionThreads;
  }

  public RuntimeAttributes.ResourceType getExecutorType() {
    return executorType;
  }
}