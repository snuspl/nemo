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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.composite;

import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.*;

import java.util.Arrays;

/**
 * Pass for initiating DAG ExecutionProperties with default values.
 */
public final class InitiationPass extends CompositePass {
  public static final String SIMPLE_NAME = "InitiationPass";

  public InitiationPass() {
    super(Arrays.asList(
        new ParallelismPass(),
        new DefaultExecutorPlacementPropertyPass(),
        new DefaultPartitioningPropertyPass(),
        new DefaultDataFlowModelPropertyPass(),
        new DefaultDataStorePropertyPass()
    ));
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }
}
