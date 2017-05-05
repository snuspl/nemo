package edu.snu.vortex.runtime.master.resourcemanager;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;

import java.io.Serializable;

/**
 * {@inheritDoc}
 * Serves as a {@link ResourceManager} in a single machine, local Runtime.
 */
public final class LocalResourceManager implements ResourceManager {

  private final Scheduler scheduler;
  private final MessageEnvironment messageEnvironment;

  public LocalResourceManager(final Scheduler scheduler,
                              final MessageEnvironment messageEnvironment) {
    this.scheduler = scheduler;
    this.messageEnvironment = messageEnvironment;
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

    // TODO #83: Introduce Task Group Executor
    // Instantiate executors.

    scheduler.onExecutorAdded(executorRepresenter);
  }
}
