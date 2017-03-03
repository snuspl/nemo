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
package edu.snu.vortex.compiler.optimizer;

import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.optimizer.passes.*;
import edu.snu.vortex.utils.Pair;

import java.util.*;

/**
 * Optimizer class.
 */
public final class Optimizer {
  /**
   * Optimize function.
   * @param dag input DAG.
   * @param policyType type of the instantiation policy that we want to use to optimize the DAG.
   * @return optimized DAG, tagged with attributes.
   * @throws Exception throws an exception if there is an exception.
   */
  public DAG optimize(final DAG dag, final PolicyType policyType) throws Exception {
    final Policy policy = new Policy(POLICIES.get(policyType));
    return policy.process(dag);
  }

  /**
   * Policy class.
   * It contains an operator pass and an edge pass, and runs them sequentially to optimize the DAG.
   */
  private static final class Policy {
    private final OperatorPass operatorPass;
    private final EdgePass edgePass;

    private Policy(final Pair<OperatorPass, EdgePass> pair) {
      this.operatorPass = pair.left();
      this.edgePass = pair.right();
    }

    private DAG process(final DAG dag) throws Exception {
      DAG operatorPlacedDAG = operatorPass.process(dag);
      DAG operatorAndEdgePlacedDAG = edgePass.process(operatorPlacedDAG);
      return operatorAndEdgePlacedDAG;
    }
  }

  /**
   * Enum for different types of instantiation policies.
   */
  public enum PolicyType {
    Pado,
    Disaggregation,
  }

  /**
   * A HashMap to match each of instantiation policies with a combination of instantiation passes.
   * As you can infer here, each instantiation policy is consisted of a pair of instantiation passes.
   */
  private static final Map<PolicyType, Pair<OperatorPass, EdgePass>> POLICIES = new HashMap<>();
  static {
    POLICIES.put(PolicyType.Pado, Pair.of(new PadoOperatorPass(), new PadoEdgePass()));
    POLICIES.put(PolicyType.Disaggregation, Pair.of(new DisaggregationOperatorPass(), new DisaggregationEdgePass()));
  }
}
