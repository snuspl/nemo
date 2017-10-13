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

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.Pair;
import edu.snu.vortex.common.PubSubEventHandlerWrapper;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.backend.vortex.VortexBackend;
import edu.snu.vortex.compiler.eventhandler.DynamicOptimizationEventHandler;
import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.frontend.beam.BeamFrontend;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.compiler.optimizer.policy.Policy;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * Compiles and runs User application.
 */
public final class UserApplicationRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(UserApplicationRunner.class.getName());

  private final String dagDirectory;
  private final String className;
  private final String[] arguments;
  private final String optimizationPolicyCanonicalName;

  private final RuntimeMaster runtimeMaster;
  private final Frontend frontend;
  private final Backend<PhysicalPlan> backend;
  private final PubSubEventHandlerWrapper pubSubEventHandlerWrapper;

  @Inject
  private UserApplicationRunner(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                                @Parameter(JobConf.UserMainClass.class) final String className,
                                @Parameter(JobConf.UserMainArguments.class) final String arguments,
                                @Parameter(JobConf.OptimizationPolicy.class) final String optimizationPolicy,
                                final PubSubEventHandlerWrapper pubSubEventHandlerWrapper,
                                final DynamicOptimizationEventHandler dynamicOptimizationEventHandler,
                                final RuntimeMaster runtimeMaster) {
    this.dagDirectory = dagDirectory;
    this.className = className;
    this.arguments = arguments.split(" ");
    this.optimizationPolicyCanonicalName = optimizationPolicy;
    this.runtimeMaster = runtimeMaster;
    this.frontend = new BeamFrontend();
    this.backend = new VortexBackend();
    this.pubSubEventHandlerWrapper = pubSubEventHandlerWrapper;
    pubSubEventHandlerWrapper.getPubSubEventHandler()
        .subscribe(dynamicOptimizationEventHandler.getEventClass(), dynamicOptimizationEventHandler);
  }

  @Override
  public void run() {
    try {
      LOG.info("##### VORTEX Compiler #####");

      final Pair<DAG<IRVertex, IREdge>, Policy> dagPolicyPair =
          clientSideCompilation(className, arguments, optimizationPolicyCanonicalName, dagDirectory);
      final DAG<IRVertex, IREdge> dag = dagPolicyPair.left();
      final Policy optimizationPolicy = dagPolicyPair.right();

      final DAG<IRVertex, IREdge> optimizedDAG = Optimizer.optimize(dag, optimizationPolicy, dagDirectory);
      optimizedDAG.storeJSON(dagDirectory, "ir-" + optimizationPolicy.getClass().getSimpleName(),
          "IR optimized for " + optimizationPolicy.getClass().getSimpleName());

      final PhysicalPlan physicalPlan = backend.compile(optimizedDAG);

      physicalPlan.getStageDAG().storeJSON(dagDirectory, "plan", "physical execution plan by compiler");
      runtimeMaster.execute(physicalPlan, frontend.getClientEndpoint());
      runtimeMaster.terminate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Pair<DAG<IRVertex, IREdge>, Policy> clientSideCompilation(final String className,
                                                                           final String[] arguments,
                                                                           final String optimizationPolicy,
                                                                           final String dagDirectory) throws Exception {
    final DAG<IRVertex, IREdge> dag = new BeamFrontend().compile(className, arguments);
    dag.storeJSON(dagDirectory, "ir", "IR before optimization");

    final Policy derivedPolicy = (Policy) Class.forName(optimizationPolicy).newInstance();
    return Pair.of(dag, derivedPolicy);
  }
}
