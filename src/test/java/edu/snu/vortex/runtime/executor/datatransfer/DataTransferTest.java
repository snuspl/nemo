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

import com.google.protobuf.ByteString;
import edu.snu.vortex.compiler.frontend.Coder;
import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.frontend.beam.coder.BeamCoder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.Reader;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.common.message.local.LocalMessageSender;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeBoundedSourceVertex;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.common.plan.logical.Stage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;
import edu.snu.vortex.runtime.executor.block.LocalStore;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import edu.snu.vortex.runtime.master.scheduler.BatchScheduler;
import edu.snu.vortex.runtime.master.scheduler.PendingTaskGroupQueue;
import edu.snu.vortex.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link InputReader} and {@link OutputWriter}.
 */
public final class DataTransferTest {
  private static final String EXECUTOR_ID_PREFIX = "Executor";
  private static final String EMPTY_DAG_DIRECTORY = "";
  private static final int EXECUTOR_CAPACITY = 1;
  private static final int SCHEDULE_TIMEOUT = 1000;
  private static final RuntimeAttribute STORE = RuntimeAttribute.Local;
  private static final int PARALLELISM_TEN = 10;

  private final Map<BlockManagerWorker, LocalMessageSender> workerToMessageSender = new HashMap<>();
  private BlockManagerMaster master;
  private BlockManagerWorker worker1;
  private BlockManagerWorker worker2;
  private Scheduler scheduler;

  @Before
  public void setUp() {
    final LocalMessageDispatcher messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    this.scheduler = new BatchScheduler(new RoundRobinSchedulingPolicy(SCHEDULE_TIMEOUT), new PendingTaskGroupQueue());
    final AtomicInteger executorCount = new AtomicInteger(0);
    final BlockManagerMaster master = new BlockManagerMaster();

    // Unused, but necessary for wiring up the message environments
    final RuntimeMaster runtimeMaster = new RuntimeMaster(scheduler, messageEnvironment, master, EMPTY_DAG_DIRECTORY);

    this.master = master;
    this.worker1 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), messageDispatcher);
    this.worker2 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), messageDispatcher);
  }

  private BlockManagerWorker createWorker(final String executorId, final LocalMessageDispatcher messageDispatcher) {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final LocalMessageEnvironment messageEnvironment = new LocalMessageEnvironment(executorId, messageDispatcher);
    final PersistentConnectionToMaster conToMaster = new PersistentConnectionToMaster(messageEnvironment);
    final BlockManagerWorker blockManagerWorker = new BlockManagerWorker(executorId, new InjectionFuture<>(injector,
        Executor.class), new LocalStore(), new PersistentConnectionToMaster(messageEnvironment), messageEnvironment);

    // Unused, but necessary for wiring up the message environments
    final Executor executor = new Executor(
        executorId,
        EXECUTOR_CAPACITY,
        conToMaster,
        messageEnvironment,
        blockManagerWorker,
        new DataTransferFactory(blockManagerWorker));
    injector.bindVolatileInstance(Executor.class, executor);

    final LocalMessageSender messageSender = new LocalMessageSender(MessageEnvironment.MASTER_COMMUNICATION_ID,
        executorId, MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER, messageDispatcher);
    workerToMessageSender.put(blockManagerWorker, messageSender);

    return blockManagerWorker;
  }

  @Test
  public void testOneToOneSameWorker() {
    writeAndRead(worker1, worker1, RuntimeAttribute.OneToOne);
  }

  @Test
  public void testOneToOneDifferentWorker() {
    // writeAndRead(worker1, worker2, RuntimeAttribute.OneToOne);
  }

  @Test
  public void testOneToManySameWorker() {
    // writeAndRead(worker1, worker1, RuntimeAttribute.Broadcast);
  }

  @Test
  public void testOneToManyDifferentWorker() {
    // writeAndRead(worker1, worker2, RuntimeAttribute.Broadcast);
  }

  @Test
  public void testManyToManySameWorker() {
    // writeAndRead(worker1, worker1, RuntimeAttribute.ScatterGather);
  }

  @Test
  public void testManyToManyDifferentWorker() {
    // writeAndRead(worker1, worker2, RuntimeAttribute.ScatterGather);
  }

  private void writeAndRead(final BlockManagerWorker sender,
                            final BlockManagerWorker receiver,
                            final RuntimeAttribute commPattern) {
    // Src setup
    final BoundedSource s = mock(BoundedSource.class);
    final RuntimeAttributeMap srcVertexAttributes = new RuntimeAttributeMap();
    srcVertexAttributes.put(RuntimeAttribute.IntegerKey.Parallelism, PARALLELISM_TEN);
    final BoundedSourceVertex v1 = new BoundedSourceVertex<>(s);
    final RuntimeVertex srcVertex = new RuntimeBoundedSourceVertex(v1, srcVertexAttributes);
    final BoundedSourceTask srcTask = new BoundedSourceTask(RuntimeIdGenerator.generateTaskId(), "srcTask", 0, null);
    final TaskGroup srcTaskGroup = new TaskGroup("srcTaskGroup", "srcStage",
        new DAGBuilder().addVertex(srcTask).build(), RuntimeAttribute.Reserved);
    final PhysicalStage srcPhysicalStage = new PhysicalStage("srcStage-0", Collections.singletonList(srcTaskGroup));

    // Dst setup
    final RuntimeAttributeMap dstVertexAttributes = new RuntimeAttributeMap();
    dstVertexAttributes.put(RuntimeAttribute.IntegerKey.Parallelism, PARALLELISM_TEN);
    final BoundedSourceVertex v2 = new BoundedSourceVertex<>(s);
    final RuntimeVertex dstVertex = new RuntimeBoundedSourceVertex(v2, dstVertexAttributes);
    final BoundedSourceTask dstTask = new BoundedSourceTask(RuntimeIdGenerator.generateTaskId(), "dstTask", 0, null);
    final TaskGroup dstTaskGroup = new TaskGroup("dstTaskGroup", "dstStage",
        new DAGBuilder().addVertex(dstTask).build(), RuntimeAttribute.Reserved);
    final PhysicalStage dstPhysicalStage = new PhysicalStage("dstStage-1", Collections.singletonList(dstTaskGroup));

    // Edge setup
    final String edgeId = "Dummy";
    final RuntimeAttributeMap edgeAttributes = new RuntimeAttributeMap();
    edgeAttributes.put(RuntimeAttribute.Key.CommPattern, commPattern);
    edgeAttributes.put(RuntimeAttribute.Key.Partition, RuntimeAttribute.Hash);
    edgeAttributes.put(RuntimeAttribute.Key.BlockStore, STORE);
    final Coder coder = new BeamCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));
    final RuntimeEdge<RuntimeVertex> dummyEdge = new RuntimeEdge<>(edgeId, edgeAttributes, srcVertex, dstVertex, coder);
    final PhysicalStageEdge physicalStageEdge = new PhysicalStageEdge(edgeId, edgeAttributes, srcVertex, dstVertex,
        null, srcPhysicalStage, dstPhysicalStage, coder);

    // Prepare task group
    final PhysicalPlan physicalPlan = new PhysicalPlan("plan", new DAGBuilder().addVertex(srcPhysicalStage)
        .addVertex(dstPhysicalStage).connectVertices(physicalStageEdge).build());
    final ScheduledTaskGroup srcScheduledTaskGroup = new ScheduledTaskGroup(srcPhysicalStage.getTaskGroupList().get(0),
        Collections.emptyList(), Collections.singletonList(physicalStageEdge));
    final ScheduledTaskGroup dstScheduledTaskGroup = new ScheduledTaskGroup(dstPhysicalStage.getTaskGroupList().get(0),
        Collections.singletonList(physicalStageEdge), Collections.emptyList());
    scheduler.scheduleJob(physicalPlan, master);

    // Schedule task group
    final ControlMessage.Message srcSetupMessage = ControlMessage.Message.newBuilder()
        .setId(0)
        .setType(ControlMessage.MessageType.ScheduleTaskGroup)
        .setScheduleTaskGroupMsg(
            ControlMessage.ScheduleTaskGroupMsg.newBuilder()
                .setTaskGroup(ByteString.copyFrom(SerializationUtils.serialize(srcScheduledTaskGroup)))
                .build())
        .build();
    final ControlMessage.Message dstSetupMessage = ControlMessage.Message.newBuilder()
        .setId(0)
        .setType(ControlMessage.MessageType.ScheduleTaskGroup)
        .setScheduleTaskGroupMsg(
            ControlMessage.ScheduleTaskGroupMsg.newBuilder()
                .setTaskGroup(ByteString.copyFrom(SerializationUtils.serialize(dstScheduledTaskGroup)))
                .build())
        .build();
    workerToMessageSender.get(sender).send(srcSetupMessage);
    workerToMessageSender.get(receiver).send(dstSetupMessage);

    // Initialize states in Master
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      if (commPattern == RuntimeAttribute.ScatterGather) {
        IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex ->
            master.initializeState(edgeId, srcTaskIndex, dstTaskIndex));
      } else {
        master.initializeState(edgeId, srcTaskIndex);
      }
    });

    // Write
    final List<List<Element>> dataWrittenList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      final List<Element> dataWritten = getListOfZeroToNine();
      final OutputWriter writer = new OutputWriter(srcTaskIndex, dstVertex, dummyEdge, sender);
      writer.write(dataWritten);
      dataWrittenList.add(dataWritten);
    });

    // Read
    final List<List<Element>> dataReadList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
      final InputReader reader = new InputReader(dstTaskIndex, srcVertex, dummyEdge, receiver);
      final List<Element> dataRead = new ArrayList<>();
      reader.read().forEach(dataRead::add);
      dataReadList.add(dataRead);
    });

    // Compare (should be the same)
    final List<Element> flattenedWrittenData = flatten(dataWrittenList);
    final List<Element> flattenedReadData = flatten(dataReadList);
    if (commPattern == RuntimeAttribute.Broadcast) {
      final List<Element> broadcastedWrittenData = new ArrayList<>();
      IntStream.range(0, PARALLELISM_TEN).forEach(i -> broadcastedWrittenData.addAll(flattenedWrittenData));
      assertEquals(broadcastedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(broadcastedWrittenData.remove(rData)));
    } else {
      assertEquals(flattenedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(flattenedWrittenData.remove(rData)));
    }
  }

  private List<Element> getListOfZeroToNine() {
    final List<Element> dummy = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(number -> dummy.add(new BeamElement<>(KV.of(number, number))));
    return dummy;
  }

  private List<Element> flatten(final List<List<Element>> listOfList) {
    return listOfList.stream().flatMap(list -> list.stream()).collect(Collectors.toList());
  }
}