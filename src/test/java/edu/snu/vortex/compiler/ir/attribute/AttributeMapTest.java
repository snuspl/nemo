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
package edu.snu.vortex.compiler.ir.attribute;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.attribute.edge.DataFlowModel;
import edu.snu.vortex.compiler.ir.attribute.edge.DataStore;
import edu.snu.vortex.compiler.ir.attribute.edge.Partitioning;
import edu.snu.vortex.compiler.ir.attribute.vertex.Parallelism;
import edu.snu.vortex.compiler.optimizer.examples.EmptyComponents;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test {@link AttributeMap}.
 */
public class AttributeMapTest {
  private final IRVertex source = new BoundedSourceVertex<>(new EmptyComponents.EmptyBoundedSource("Source"));
  private final IRVertex destination = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements"));
  private final IREdge edge = new IREdge(IREdge.Type.OneToOne, source, destination, Coder.DUMMY_CODER);

  private AttributeMap edgeMap;
  private AttributeMap vertexMap;

  @Before
  public void setUp() {
    this.edgeMap = AttributeMap.of(edge);
    this.vertexMap = AttributeMap.of(source);
  }

  @Test
  public void testDefaultValues() {
    assertEquals(Partitioning.HASH, edgeMap.getStringAttr(ExecutionFactor.Type.Partitioning));
    assertEquals(1, (long) vertexMap.getIntegerAttr(ExecutionFactor.Type.Parallelism));
    assertEquals(edge.getId(), edgeMap.getId());
    assertEquals(source.getId(), vertexMap.getId());
  }

  @Test
  public void testPutGetAndRemove() {
    edgeMap.put(DataStore.of(DataStore.MEMORY));
    assertEquals(DataStore.MEMORY, edgeMap.getStringAttr(ExecutionFactor.Type.DataStore));
    edgeMap.put(DataFlowModel.of(DataFlowModel.PULL));
    assertEquals(DataFlowModel.PULL, edgeMap.getStringAttr(ExecutionFactor.Type.DataFlowModel));

    edgeMap.remove(ExecutionFactor.Type.DataFlowModel);
    assertNull(edgeMap.get(ExecutionFactor.Type.DataFlowModel));

    vertexMap.put(Parallelism.of(100));
    assertEquals(100, (long) vertexMap.getIntegerAttr(ExecutionFactor.Type.Parallelism));
  }
}
