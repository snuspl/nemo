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
package edu.snu.vortex.compiler.frontend.beam;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.runtime.common.plan.logical.LogicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.logical.Stage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.vortex.runtime.common.state.JobState;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import edu.snu.vortex.runtime.master.JobStateManager;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.apache.beam.sdk.PipelineResult;
import org.joda.time.Duration;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test {@link Result}.
 */
public class ResultTest {

  @Test(timeout = 1000)
  public void testState() throws Exception {
    final Result result = new Result();
    assertTrue(result.getState() == PipelineResult.State.UNKNOWN);

    // Create a JobStateManager of an empty dag.
    final DAGBuilder<IRVertex, IREdge> irDagBuilder = new DAGBuilder<>();
    final DAG<IRVertex, IREdge> irDAG = irDagBuilder.build();
    final DAG<Stage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator());
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());
    final JobStateManager jobStateManager =
        new JobStateManager(new PhysicalPlan("TestPlan", physicalDAG), new BlockManagerMaster());

    // Set it and get the current state.
    result.setJobStateManager(jobStateManager);
    assertTrue(result.getState() == PipelineResult.State.RUNNING);

    // Wait for the pipeline to finish.
    // It have to be still running.
    assertTrue(result.waitUntilFinish(Duration.millis(100)) == PipelineResult.State.RUNNING);

    jobStateManager.onJobStateChanged(JobState.State.COMPLETE);
    assertTrue(result.waitUntilFinish() == PipelineResult.State.DONE);
  }
}
