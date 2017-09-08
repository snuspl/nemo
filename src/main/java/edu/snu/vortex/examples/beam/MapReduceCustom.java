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
package edu.snu.vortex.examples.beam;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.compiler.optimizer.passes.DefaultStagePartitioningPass;
import edu.snu.vortex.compiler.optimizer.passes.ScheduleGroupPass;
import edu.snu.vortex.compiler.optimizer.passes.StaticOptimizationPass;

/**
 * Sample MapReduce application, running a custom pass.
 */
public final class MapReduceCustom {
  /**
   * Private Constructor.
   */
  private MapReduceCustom() {
  }

  /**
   * Main function for the MR BEAM program running a custom pass.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    // register a new pass to the Optimizer before running the application.
    Optimizer.registerPass("custom", new CustomDefaultPass());
    // Run the same original program
    MapReduce.main(args);
  }

  /**
   * A custom pass that consists of two passes that stage-partitions and puts schedule group numbers on the DAG.
   * This is just a simple demonstration of a pass, but it can be flexibly modified for other uses.
   */
  private static final class CustomDefaultPass implements StaticOptimizationPass {
    @Override
    public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
      return new ScheduleGroupPass().process(new DefaultStagePartitioningPass().process(dag));
    }
  }
}
