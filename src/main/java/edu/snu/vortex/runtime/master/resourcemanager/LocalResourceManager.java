package edu.snu.vortex.runtime.master.resourcemanager;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * {@inheritDoc}
 * Serves as a {@link ResourceManager} in a single machine, local Runtime.
 */
public final class LocalResourceManager implements ResourceManager {

  private final Scheduler scheduler;
  private final Map<String, Executor> executorMap;
  private final MessageEnvironment messageEnvironment;
  private final BlockManagerMaster blockManagerMaster;

  public LocalResourceManager(final Scheduler scheduler,
                              final MessageEnvironment messageEnvironment,
                              final BlockManagerMaster blockManagerMaster) {
    this.scheduler = scheduler;
    this.messageEnvironment = messageEnvironment;
    this.blockManagerMaster = blockManagerMaster;
    this.executorMap = new HashMap<>();
  }

  @Override
  public synchronized void requestExecutor(final RuntimeAttribute resourceType, final int executorCapacity) {
    final String executorId = RuntimeIdGenerator.generateExecutorId();
    final MessageSender<Serializable> messageSender;
    try {
      messageSender = messageEnvironment.asyncConnect(executorId, "TaskGroupMessage").get();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final ExecutorRepresenter executorRepresenter =
        new ExecutorRepresenter(executorId, resourceType, executorCapacity, messageSender);
    final Executor executor = new Executor(executorId, executorCapacity, messageEnvironment, blockManagerMaster);
    executorMap.put(executorId, executor);
    scheduler.onExecutorAdded(executorRepresenter);
  }
}
