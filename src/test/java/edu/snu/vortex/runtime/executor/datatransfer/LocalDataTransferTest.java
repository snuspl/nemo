/*
 * Copyright (C) 2016 Seoul National University
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

import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;
import edu.snu.vortex.runtime.executor.block.LocalBlockPlacement;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link InputReader} and {@link OutputWriter}.
 */
public final class LocalDataTransferTest {
  private BlockManagerMaster master;
  private BlockManagerWorker worker;

  @Before
  public void setUp() {
    this.master = new BlockManagerMaster();
    this.worker = new BlockManagerWorker(master, new LocalBlockPlacement());
  }

  @Test
  public void testOneToOne() {
    // Src setup
    final int srcTaskIndex = 0;
    final RuntimeAttributeMap srcVertexAttributes = new RuntimeAttributeMap();

    // Dst setup
    final int dstTaskIndex = srcTaskIndex;
    final RuntimeAttributeMap dstVertexAttributes = new RuntimeAttributeMap();
    dstVertexAttributes.put(RuntimeAttribute.IntegerKey.Parallelism, 1);

    // Edge setup
    final String edgeId = "Dummy";
    final RuntimeAttributeMap edgeAttributes = new RuntimeAttributeMap();
    edgeAttributes.put(RuntimeAttribute.Key.CommPattern, RuntimeAttribute.OneToOne);
    edgeAttributes.put(RuntimeAttribute.Key.Partition, RuntimeAttribute.Hash);
    edgeAttributes.put(RuntimeAttribute.Key.BlockPlacement, RuntimeAttribute.Local);

    // Write
    final OutputWriter writer = new OutputWriter(edgeId, srcTaskIndex, dstVertexAttributes, edgeAttributes, worker);
    final List<Element> dataWritten = new ArrayList<>();
    dataWritten.add(new BeamElement<>(1));
    dataWritten.add(new BeamElement<>(2));
    dataWritten.add(new BeamElement<>(3));
    dataWritten.add(new BeamElement<>(4));
    writer.write(dataWritten);

    // Read
    final InputReader reader = new InputReader(edgeId, dstTaskIndex, srcVertexAttributes, edgeAttributes, worker);
    final List<Element> dataRead = new ArrayList<>();
    reader.read().forEach(dataRead::add);

    // Compare (should be the same)
    assertTrue(dataWritten.equals(dataRead));
  }

  @Test
  public void testOneToMany() {
  }

  @Test
  public void testManyToMany() {
  }

}