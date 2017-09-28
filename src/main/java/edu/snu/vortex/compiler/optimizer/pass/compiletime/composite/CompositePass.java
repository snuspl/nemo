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

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.CompileTimePass;

import java.util.*;

/**
 * A compile-time pass composed of multiple passes.
 */
public abstract class CompositePass implements CompileTimePass {
  private final List<CompileTimePass> passList;
  private final Set<ExecutionProperty.Key> prerequisiteExecutionProperties;

  CompositePass(List<CompileTimePass> passList) {
    this.passList = passList;
    this.prerequisiteExecutionProperties = new HashSet<>();
    passList.forEach(pass -> prerequisiteExecutionProperties.addAll(pass.getPrerequisiteExecutionProperties()));
  }

  CompositePass(List<CompileTimePass> passList,
                Set<ExecutionProperty.Key> prerequisiteExecutionProperties) {
    this.passList = passList;
    this.prerequisiteExecutionProperties = prerequisiteExecutionProperties;
    passList.forEach(pass -> prerequisiteExecutionProperties.addAll(pass.getPrerequisiteExecutionProperties()));
  }

  public List<CompileTimePass> getPassList() {
    return passList;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(DAG<IRVertex, IREdge> irVertexIREdgeDAG) {
    return recursivelyApply(irVertexIREdgeDAG, getPassList().iterator());
  }

  private DAG<IRVertex, IREdge> recursivelyApply(DAG<IRVertex, IREdge> dag, Iterator<CompileTimePass> passIterator) {
    if (passIterator.hasNext()) {
      return recursivelyApply(passIterator.next().apply(dag), passIterator);
    } else {
      return dag;
    }
  }

  @Override
  public Set<ExecutionProperty.Key> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }
}
