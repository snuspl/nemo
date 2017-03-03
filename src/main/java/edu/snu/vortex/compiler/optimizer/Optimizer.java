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

import com.sun.tools.javac.util.Pair;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.optimizer.passes.*;

import java.util.*;

/**
 * Optimizer class.
 */
public final class Optimizer {
  /**
   * Optimize function.
   * @param dag input DAG.
   * @return optimized DAG, tagged with attributes.
   */
  public DAG optimize(final DAG dag, final PolicyType policyType) throws Exception {
    final Policy policy = new Policy(POLICIES.get(policyType));
    return policy.process(dag);
  }

  private static class Policy {
    private final OperatorPass operPass;
    private final EdgePass edgePass;

    private Policy(final Pair<OperatorPass, EdgePass> pair) {
      this.operPass = pair.fst;
      this.edgePass = pair.snd;
    }

    private DAG process(final DAG dag) throws Exception {
      DAG operatorPlacedDAG = operPass.process(dag);
      DAG placedDAG = edgePass.process(operatorPlacedDAG);
      return placedDAG;
    }
  }

  public enum PolicyType {
    Pado,
    Disaggregation,
  }

  private static final Map<PolicyType, Pair<OperatorPass, EdgePass>> POLICIES = new HashMap<>();
  static {
    POLICIES.put(PolicyType.Pado, Pair.of(new PadoOperatorPass(), new PadoEdgePass()));
    POLICIES.put(PolicyType.Disaggregation, Pair.of(new DisaggregationOperatorPass(), new DisaggregationEdgePass()));
  }
}
