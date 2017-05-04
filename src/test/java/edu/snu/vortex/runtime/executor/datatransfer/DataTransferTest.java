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
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;
import edu.snu.vortex.runtime.executor.block.LocalStore;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests {@link InputReader} and {@link OutputWriter}.
 */
public final class DataTransferTest {
  private BlockManagerMaster master;
  private BlockManagerWorker worker1;
  private BlockManagerWorker worker2;

  @Before
  public void setUp() {
    this.master = new BlockManagerMaster();
    this.worker1 = new BlockManagerWorker("worker1", master, new LocalStore());
    this.worker2 = new BlockManagerWorker("worker2", master, new LocalStore());
    this.master.addNewWorker(worker1);
    this.master.addNewWorker(worker2);
  }

  @Test
  public void testOneToOneSameWorker() {
    testOneToOne(worker1, worker1);
  }

  @Test
  public void testOneToOneDifferentWorker() {
    testOneToOne(worker1, worker2);
  }

  @Test
  public void testOneToMany() {
  }

  @Test
  public void testManyToMany() {
  }

  private void testOneToOne(final BlockManagerWorker sender,
                            final BlockManagerWorker receiver) {
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
    edgeAttributes.put(RuntimeAttribute.Key.BlockStore, RuntimeAttribute.Local);

    // Initialize states in BlockManagerMaster
    master.initializeState(edgeId, srcTaskIndex);

    // Write from worker1
    final OutputWriter writer = new OutputWriter(edgeId, srcTaskIndex, dstVertexAttributes, edgeAttributes, sender);
    final List<Element> dataWritten = new ArrayList<>();
    dataWritten.add(new BeamElement<>(1));
    dataWritten.add(new BeamElement<>(2));
    dataWritten.add(new BeamElement<>(3));
    dataWritten.add(new BeamElement<>(4));
    writer.write(dataWritten);

    // Read from worker2
    final InputReader reader = new InputReader(edgeId, dstTaskIndex, srcVertexAttributes, edgeAttributes, receiver);
    final List<Element> dataRead = new ArrayList<>();
    reader.read().forEach(dataRead::add);

    // Compare (should be the same)
    assertTrue(dataWritten.equals(dataRead));

    // Block Location
    master.getBlockLocation(RuntimeIdGenerator.generateBlockId(edgeId, srcTaskIndex));
  }

}