package edu.snu.vortex.runtime.master.resourcemanager;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * {@inheritDoc}
 * Serves as a {@link ResourceManager} in a single machine, local Runtime.
 */
public final class LocalResourceManager implements ResourceManager {

  private final Scheduler scheduler;
  private final Map<String, Executor> executorMap;
  private final Map<String, MessageEnvironment<ControlMessage.Message>> executorMessageEnvMap;
  private final MessageEnvironment<ControlMessage.Message> driverMessageEnvironment;
  private final BlockManagerMaster blockManagerMaster;

  public LocalResourceManager(final Scheduler scheduler,
                              final MessageEnvironment<ControlMessage.Message> driverMessageEnvironment,
                              final BlockManagerMaster blockManagerMaster) {
    this.scheduler = scheduler;
    this.driverMessageEnvironment = driverMessageEnvironment;
    this.blockManagerMaster = blockManagerMaster;
    this.executorMap = new HashMap<>();
    this.executorMessageEnvMap = new HashMap<>();
  }

  @Override
  public synchronized void requestExecutor(final RuntimeAttribute resourceType, final int executorCapacity) {
    final String executorId = RuntimeIdGenerator.generateExecutorId();
    final Map<String, MessageSender<ControlMessage.Message>> nodeIdToMsgSenderMap = new HashMap<>();

    // Initiate message environment for the executor, and create the executor!
    final LocalMessageEnvironment executorMessageEnvironment = new LocalMessageEnvironment(executorId);
    final Executor executor =
        new Executor(executorId, executorCapacity,
            executorMessageEnvironment, nodeIdToMsgSenderMap, blockManagerMaster);
    executorMap.put(executorId, executor);
//    executorMessageEnvMap.forEach((id, msgEnv) -> {
//      try {
//        // Connect to the existing executors
//        final MessageSender<ControlMessage.Message> messageSender =
//            executorMessageEnvironment.asyncConnect(id, MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER).get();
//        nodeIdToMsgSenderMap.put(id, messageSender);
//
//        // Listen to the existing executors
//        msgEnv.asyncConnect()
//
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      } catch (ExecutionException e) {
//        e.printStackTrace();
//      }
//    });


    //

    // Connect to the executor and initiate Master side's executor representation.
    final MessageSender<ControlMessage.Message> messageSender;
    try {
      messageSender =
          driverMessageEnvironment.asyncConnect(executorId, MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER).get();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    final ExecutorRepresenter executorRepresenter =
        new ExecutorRepresenter(executorId, resourceType, executorCapacity, messageSender);
    scheduler.onExecutorAdded(executorRepresenter);
  }
}
