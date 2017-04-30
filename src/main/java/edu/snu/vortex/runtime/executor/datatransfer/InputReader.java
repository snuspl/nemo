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
package edu.snu.vortex.runtime.executor.datatransfer;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.executor.dataplacement.DataPlacement;

import java.util.ArrayList;
import java.util.List;

/**
 * Input channel interface.
 */
public final class InputReader extends DataTransfer {

  private final Task dstTask;
  private final RuntimeVertex srcRuntimeVertex;
  private final RuntimeEdge runtimeEdge;
  private final DataPlacement dataPlacement;

  public InputReader(final Task dstTask,
                     final RuntimeVertex srcRuntimeVertex,
                     final RuntimeEdge runtimeEdge,
                     final DataPlacement dataPlacement) {
    super(runtimeEdge.getRuntimeEdgeId());
    this.dstTask = dstTask;
    this.srcRuntimeVertex = srcRuntimeVertex;
    this.runtimeEdge = runtimeEdge;
    this.dataPlacement = dataPlacement;
  }

  public Iterable<Element> read() {
    switch (srcRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.Key.CommPattern)) {
    case OneToOne:
      return readOneToOne();
    case Broadcast:
      return readBroadcast();
    case ScatterGather:
      return readScatterGather();
    default:
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  public Iterable<Element> readOneToOne() {
    return dataPlacement.get(runtimeEdge.getRuntimeEdgeId(), dstTask.getIndex());
  }

  public Iterable<Element> readBroadcast() {
    final int numSrcTasks = srcRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.IntegerKey.Parallelism);

    final List<Element> readData = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final Iterable<Element> dataFromATask = dataPlacement.get(runtimeEdge.getRuntimeEdgeId(), srcTaskIdx);
      dataFromATask.forEach(element -> readData.add(element));
    }
    return readData;
  }

  public Iterable<Element> readScatterGather() {
    final int numSrcTasks = srcRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.IntegerKey.Parallelism);

    final List<Element> readData = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final Iterable<Element> dataFromATask =
          dataPlacement.get(runtimeEdge.getRuntimeEdgeId(), srcTaskIdx, dstTask.getIndex());
      dataFromATask.forEach(element -> readData.add(element));
    }
    return readData;
  }
}
