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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.comm.RtControllable;
import edu.snu.vortex.runtime.common.config.RtConfig;
import edu.snu.vortex.runtime.common.execplan.ExecutionPlan;
import edu.snu.vortex.runtime.common.execplan.RuntimeAttributes;
import edu.snu.vortex.runtime.master.transfer.DataTransferManagerMaster;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * RtMaster.
 */
public class RtMaster {
  private static final Logger LOG = Logger.getLogger(RtMaster.class.getName());

  private final RtConfig rtConfig;
  private static final RtConfig.RtExecMode DEFAULT_RUNTIME_EXECUTION_MODE = RtConfig.RtExecMode.STREAM;

  private final Scheduler scheduler;
  private final ResourceManager resourceManager;
  private final ExecutionStateManager executionStateManager;
  private final MasterCommunicator masterCommunicator;
  private final DataTransferManagerMaster dataTransferManagerMaster;

  public RtMaster() {
    this.rtConfig = new RtConfig(DEFAULT_RUNTIME_EXECUTION_MODE);
    this.scheduler = new Scheduler();
    this.resourceManager = new ResourceManager();
    this.executionStateManager = new ExecutionStateManager();
    this.masterCommunicator = new MasterCommunicator();
    this.dataTransferManagerMaster = new DataTransferManagerMaster();
  }

  public void initialize()  {
    // Use default configs
    Map<RuntimeAttributes.ResourceType, Integer> defaultResources = new HashMap<>();
    defaultResources.put(RuntimeAttributes.ResourceType.TRANSIENT, 3);
    defaultResources.put(RuntimeAttributes.ResourceType.RESERVED, 1);
    resourceManager.initialize(this, rtConfig.getRtExecMode(), defaultResources, masterCommunicator);
    scheduler.initialize(masterCommunicator);
    masterCommunicator.initialize(resourceManager, executionStateManager, dataTransferManagerMaster);
    dataTransferManagerMaster.initialize(masterCommunicator);
  }

  public void submitExecutionPlan(final ExecutionPlan execPlan) {
    executionStateManager.submitExecutionPlan(execPlan);
  }

  public void onRtControllableReceived(final RtControllable rtControllable) {

  }

  public void onJobCompleted() {

  }
}
