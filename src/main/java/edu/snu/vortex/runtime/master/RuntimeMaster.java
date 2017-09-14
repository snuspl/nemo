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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.proxy.ClientEndpoint;
import edu.snu.vortex.common.proxy.DriverEndpoint;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.optimizer.passes.DataSkewPass;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.state.PartitionState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.exception.ContainerException;
import edu.snu.vortex.runtime.exception.IllegalMessageException;
import edu.snu.vortex.runtime.exception.UnknownExecutionStateException;
import edu.snu.vortex.runtime.exception.UnknownFailureCauseException;
import edu.snu.vortex.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static edu.snu.vortex.runtime.common.state.TaskGroupState.State.COMPLETE;
import static edu.snu.vortex.runtime.common.state.TaskGroupState.State.ON_HOLD;

/**
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link PhysicalPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 *    a) Scheduling the job with {@link Scheduler}.
 *    b) (Please list others done by Runtime Master as features are added).
 */
public final class RuntimeMaster {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMaster.class.getName());
  private static final int DAG_LOGGING_PERIOD = 3000;

  private final Scheduler scheduler;
  private final ContainerManager containerManager;
  private final MessageEnvironment masterMessageEnvironment;
  private final PartitionManagerMaster partitionManagerMaster;
  private JobStateManager jobStateManager;
  private final MetricMessageHandler metricMessageHandler;
  // For converting json data. This is a thread safe.
  // [Vortex-420] Create a Singleton ObjectMapper
  private final ObjectMapper objectMapper;

  private final String dagDirectory;
  private final Set<IRVertex> irVertices;
  private final int maxScheduleAttempt;

  @Inject
  public RuntimeMaster(final Scheduler scheduler,
                       final ContainerManager containerManager,
                       final MessageEnvironment masterMessageEnvironment,
                       final PartitionManagerMaster partitionManagerMaster,
                       final MetricMessageHandler metricMessageHandler,
                       final UpdatePhysicalPlanEventHandler handler,
                       @Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                       @Parameter(JobConf.MaxScheduleAttempt.class) final int maxScheduleAttempt) {
    this.scheduler = scheduler;
    this.maxScheduleAttempt = maxScheduleAttempt;
    this.containerManager = containerManager;
    this.masterMessageEnvironment = masterMessageEnvironment;
    this.masterMessageEnvironment
        .setupListener(MessageEnvironment.MASTER_MESSAGE_RECEIVER, new MasterControlMessageReceiver());
    this.partitionManagerMaster = partitionManagerMaster;
    this.dagDirectory = dagDirectory;
    this.metricMessageHandler = metricMessageHandler;
    this.irVertices = new HashSet<>();
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Submits the {@link PhysicalPlan} to Runtime.
   * @param plan to execute.
   * @param clientEndpoint of this plan.
   */
  public void execute(final PhysicalPlan plan,
                      final ClientEndpoint clientEndpoint) {
    this.irVertices.addAll(plan.getTaskIRVertexMap().values());
    try {
      jobStateManager = scheduler.scheduleJob(plan, maxScheduleAttempt);
      final DriverEndpoint driverEndpoint = new DriverEndpoint(jobStateManager, clientEndpoint);

      // Schedule dag logging thread
      final ScheduledExecutorService dagLoggingExecutor = scheduleDagLogging();

      // Wait the job to finish and stop logging
      jobStateManager.waitUntilFinish();
      dagLoggingExecutor.shutdown();

      jobStateManager.storeJSON(dagDirectory, "final");
      LOG.info("{} is complete!", plan.getId());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void terminate() {
    final Future<Boolean> allExecutorsClosed = containerManager.terminate();

    try {
      if (allExecutorsClosed.get()) {
        LOG.info("All executors were closed successfully!");
      }
    } catch (Exception e) {
      new ContainerException(new Throwable("An exception occurred while trying to terminate ContainerManager"));
      e.printStackTrace();
    }
  }

  /**
   * Handler for control messages received by Master.
   */
  public final class MasterControlMessageReceiver implements MessageListener<ControlMessage.Message> {

    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
      case TaskGroupStateChanged:
        final ControlMessage.TaskGroupStateChangedMsg taskGroupStateChangedMsg = message.getTaskStateChangedMsg();

        scheduler.onTaskGroupStateChanged(taskGroupStateChangedMsg.getExecutorId(),
            taskGroupStateChangedMsg.getTaskGroupId(),
            convertTaskGroupState(taskGroupStateChangedMsg.getState()),
            taskGroupStateChangedMsg.getAttemptIdx(),
            taskGroupStateChangedMsg.getTasksPutOnHoldIdsList(),
            convertFailureCause(taskGroupStateChangedMsg.getFailureCause()));
        break;
      case PartitionStateChanged:
        final ControlMessage.PartitionStateChangedMsg partitionStateChangedMsg = message.getPartitionStateChangedMsg();
        // process message with partition size.
        final List<Long> blockSizeInfo = partitionStateChangedMsg.getBlockSizeInfoList();
        if (!blockSizeInfo.isEmpty()) {
          final String srcVertexId = partitionStateChangedMsg.getSrcIRVertexId();
          final IRVertex vertexToSendMetricDataTo = irVertices.stream()
              .filter(irVertex -> irVertex.getId().equals(srcVertexId)).findFirst()
              .orElseThrow(() -> new RuntimeException(srcVertexId + " doesn't exist in the submitted Physical Plan"));

          if (vertexToSendMetricDataTo instanceof MetricCollectionBarrierVertex) {
            final MetricCollectionBarrierVertex<Long> metricCollectionBarrierVertex =
                (MetricCollectionBarrierVertex) vertexToSendMetricDataTo;
            metricCollectionBarrierVertex.accumulateMetric(partitionStateChangedMsg.getPartitionId(), blockSizeInfo);
          } else {
            throw new RuntimeException("Something wrong happened at " + DataSkewPass.class.getSimpleName() + ". ");
          }
        }
        partitionManagerMaster.onPartitionStateChanged(partitionStateChangedMsg.getPartitionId(),
            convertPartitionState(partitionStateChangedMsg.getState()),
            partitionStateChangedMsg.getExecutorId(),
            partitionStateChangedMsg.getSrcTaskIdx());
        break;
      case ExecutorFailed:
        final ControlMessage.ExecutorFailedMsg executorFailedMsg = message.getExecutorFailedMsg();
        final String failedExecutorId = executorFailedMsg.getExecutorId();
        final Exception exception = SerializationUtils.deserialize(executorFailedMsg.getException().toByteArray());
        LOG.error(failedExecutorId + " failed, Stack Trace: ", exception);
        containerManager.onExecutorRemoved(failedExecutorId);
        throw new RuntimeException(exception);
      case MetricMessageReceived:
        final ControlMessage.MetricMsg metricMsg = message.getMetricMsg();
        final String executorId = metricMsg.getExecutorId();
        metricMsg.getMessagesList().stream().map(new JsonStringToMapFunction())
            .forEach((msg) -> metricMessageHandler.onMetricMessageReceived(executorId, msg));
        break;
      case StoreMetadata:
        partitionManagerMaster.getMetadataManager().onStoreMetadata(message);
        break;
      case RemoveMetadata:
        partitionManagerMaster.getMetadataManager().onRemoveMetadata(message);
        break;
      default:
        throw new IllegalMessageException(
            new Exception("This message should not be received by Master :" + message.getType()));
      }
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
      case RequestPartitionLocation:
        partitionManagerMaster.onRequestPartitionLocation(message, messageContext);
        break;
      case RequestMetadata:
        partitionManagerMaster.getMetadataManager().onRequestMetadata(message, messageContext);
        break;
      case ReserveBlock:
        partitionManagerMaster.getMetadataManager().onReserveBlock(message, messageContext);
        break;
      default:
        throw new IllegalMessageException(
            new Exception("This message should not be requested to Master :" + message.getType()));
      }
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  private static TaskGroupState.State convertTaskGroupState(final ControlMessage.TaskGroupStateFromExecutor state) {
    switch (state) {
    case READY:
      return TaskGroupState.State.READY;
    case EXECUTING:
      return TaskGroupState.State.EXECUTING;
    case COMPLETE:
      return COMPLETE;
    case FAILED_RECOVERABLE:
      return TaskGroupState.State.FAILED_RECOVERABLE;
    case FAILED_UNRECOVERABLE:
      return TaskGroupState.State.FAILED_UNRECOVERABLE;
    case ON_HOLD:
      return ON_HOLD;
    default:
      throw new UnknownExecutionStateException(new Exception("This TaskGroupState is unknown: " + state));
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  public static PartitionState.State convertPartitionState(final ControlMessage.PartitionStateFromExecutor state) {
    switch (state) {
    case PARTITION_READY:
      return PartitionState.State.READY;
    case SCHEDULED:
      return PartitionState.State.SCHEDULED;
    case COMMITTED:
      return PartitionState.State.COMMITTED;
    case PARTIAL_COMMITTED:
      return PartitionState.State.PARTIAL_COMMITTED;
    case LOST_BEFORE_COMMIT:
      return PartitionState.State.LOST_BEFORE_COMMIT;
    case LOST:
      return PartitionState.State.LOST;
    case REMOVED:
      return PartitionState.State.REMOVED;
    default:
      throw new UnknownExecutionStateException(new Exception("This PartitionState is unknown: " + state));
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  public static ControlMessage.PartitionStateFromExecutor convertPartitionState(final PartitionState.State state) {
    switch (state) {
      case READY:
        return ControlMessage.PartitionStateFromExecutor.PARTITION_READY;
      case SCHEDULED:
        return ControlMessage.PartitionStateFromExecutor.SCHEDULED;
      case COMMITTED:
        return ControlMessage.PartitionStateFromExecutor.COMMITTED;
      case PARTIAL_COMMITTED:
        return ControlMessage.PartitionStateFromExecutor.PARTIAL_COMMITTED;
      case LOST_BEFORE_COMMIT:
        return ControlMessage.PartitionStateFromExecutor.LOST_BEFORE_COMMIT;
      case LOST:
        return ControlMessage.PartitionStateFromExecutor.LOST;
      case REMOVED:
        return ControlMessage.PartitionStateFromExecutor.REMOVED;
      default:
        throw new UnknownExecutionStateException(new Exception("This PartitionState is unknown: " + state));
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  private TaskGroupState.RecoverableFailureCause convertFailureCause(
      final ControlMessage.RecoverableFailureCause cause) {
    switch (cause) {
    case InputReadFailure:
      return TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE;
    case OutputWriteFailure:
      return TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE;
    default:
      throw new UnknownFailureCauseException(new Throwable("The failure cause for the recoverable failure is unknown"));
    }
  }

  /**
   * Schedules a periodic DAG logging thread.
   * TODO #58: Web UI (Real-time visualization)
   *
   * @return the scheduled executor service.
   */
  private ScheduledExecutorService scheduleDagLogging() {
    final ScheduledExecutorService dagLoggingExecutor = Executors.newSingleThreadScheduledExecutor();
    dagLoggingExecutor.scheduleAtFixedRate(new Runnable() {
      private int dagLogFileIndex = 0;

      public void run() {
        jobStateManager.storeJSON(dagDirectory, String.valueOf(dagLogFileIndex++));
      }
    }, DAG_LOGGING_PERIOD, DAG_LOGGING_PERIOD, TimeUnit.MILLISECONDS);

    return dagLoggingExecutor;
  }

  /**
   * Map function that converts a json string to a java map object.
   */
  final class JsonStringToMapFunction implements Function<String, Map<String, Object>> {
    @Override
    public Map<String, Object> apply(final String s) {
      try {
        return objectMapper.readValue(s, new TypeReference<Map<String, String>>() { });
      } catch (final IOException e) {
        e.printStackTrace();
        throw new IllegalMessageException(
            new Exception("The metric message format is incorrect. It should be in Json format: " + s));
      }
    }
  }
}
