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
package edu.snu.vortex.runtime.common.execplan;

import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.runtime.common.task.OperatorTask;

import java.util.ArrayList;
import java.util.List;

/**
 * Runtime Operator Vertex.
 */
public final class RuntimeOperatorVertex extends RuntimeVertex {
  private final List<OperatorTask> taskList;
  private final OperatorVertex operatorVertex;

  public RuntimeOperatorVertex(final OperatorVertex operatorVertex) {
    super(operatorVertex.getId());
    this.operatorVertex = operatorVertex;
    this.taskList = new ArrayList<>();
  }

  @Override
  public List<OperatorTask> getTaskList() {
    return null;
  }

  public OperatorVertex getOperatorVertex() {
    return operatorVertex;
  }
}
