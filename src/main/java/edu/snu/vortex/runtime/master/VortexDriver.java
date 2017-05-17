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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.master.resourcemanager.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.vortex.runtime.common.RuntimeAttribute.*;

/**
 * REEF Driver for Vortex.
 */
@Unit
@DriverSide
public final class VortexDriver {
  private static final Logger LOG = Logger.getLogger(VortexDriver.class.getName());
  private static final String AGGREGATOR_CONTEXT_PREFIX = "AGGREGATOR_CONTEXT_";

  private final EvaluatorRequestor evaluatorRequestor; // for requesting resources
  private final RuntimeMaster runtimeMaster; // Vortex RuntimeMaster
  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;

  // Resource configuration for single thread pool
  private final int executorNum;
  private final int executorCores;
  private final int executorMem;
  private final int executorThreads;

  private final UserMainRunner userMainRunner;
  private final PendingTaskSchedulerRunner pendingTaskSchedulerRunner;

  private final Compiler compiler;

  @Inject
  private VortexDriver(final ExecutoruatorRequestor evaluatorRequestor,
                       final MasterToAggregatorRequestor masterToAggregatorRequestor,
                       final RuntimeMaster runtimeMaster,
                       final UserMainRunner userMainRunner,
                       final PendingTaskSchedulerRunner pendingTaskSchedulerRunner,
                       final NameServer nameServer,
                       final LocalAddressProvider localAddressProvider,
                       final Compiler compiler,
                       @Parameter(JobConf.ExecutorMem.class) final int executorMem,
                       @Parameter(JobConf.ExecutorNum.class) final int executorNum,
                       @Parameter(JobConf.ExecutorCores.class) final int executorCores,
                       @Parameter(JobConf.ExecutorThreads.class) final int executorThreads) {
    this.userMainRunner = userMainRunner;
    this.pendingTaskSchedulerRunner = pendingTaskSchedulerRunner;
    this.evaluatorRequestor = evaluatorRequestor;
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.runtimeMaster = runtimeMaster;
    this.masterToAggregatorRequestor = masterToAggregatorRequestor;

    this.compiler = compiler;

    this.executorNum = executorNum;
    this.executorCores = executorCores;
    this.executorMem = executorMem;
    this.executorThreads = executorThreads;
  }

  /**
   * Initialize a default amount of resources by requesting to the Resource Manager.
   */
  private void initializeResources() {
    final Set<RuntimeAttribute> completeSetOfResourceType =
        new HashSet<>(Arrays.asList(Transient, Reserved, Compute, Storage));
    completeSetOfResourceType.forEach(resourceType -> {
      for (int i = 0; i < runtimeConfiguration.getExecutorConfiguration().getDefaultExecutorNum(); i++) {
        final Optional<Executor> executor =
            resourceManager.requestExecutor(resourceType, runtimeConfiguration.getExecutorConfiguration());

        if (executor.isPresent()) {
          // Connect to the executor and initiate Master side's executor representation.
          final MessageSender messageSender;
          try {
            messageSender =
                masterMessageEnvironment.asyncConnect(
                    executor.get().getExecutorId(), MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER).get();
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
          final ExecutorRepresenter executorRepresenter =
              new ExecutorRepresenter(executor.get().getExecutorId(), resourceType,
                  executor.get().getCapacity(), messageSender);
          scheduler.onExecutorAdded(executorRepresenter);
        }
      }
    });
  }


  /**
   * Driver started.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // Start threads
      startThreads();

      // Launch resources
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(executorNum)
          .setMemory(executorMem)
          .setNumberOfCores(executorCores)
          .build());

      // Launch job
      final ExecutionPlan executionPlan = compiler.compile();
      runtimeMaster.execute(executionPlan);
    }
  }

  /**
   * Container allocated.
   */
  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Container allocated");
      final String executorId = allocatedEvaluator.getId() + "_vortex_executor";
      final Configuration contextConfiguration = Configurations.merge(
          ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER, executorId + "_CONTEXT")
              .set(ContextConfiguration.ON_CONTEXT_STOP, VortexContextStopHandler.class)
              .build(),
          getNameResolverServiceConfiguration());
      allocatedEvaluator.submitContextAndTask(contextConfiguration, getExecutorConfiguration(executorId));
    }
  }

  private Configuration getExecutorConfiguration(final String aggregatorId) {
    final Configuration aggregatorConfiguration = VortexAggregatorConf.CONF
        .set(VortexAggregatorConf.NUM_OF_THREADS, aggregatorCores)
        .build();

    final Configuration contextConfiguration = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, aggregatorId)
        .set(ContextConfiguration.ON_CONTEXT_STOP, VortexContextStopHandler.class)
        .set(ContextConfiguration.ON_MESSAGE, VortexAggregator.ContextMessageHandler.class)
        .set(ContextConfiguration.ON_SEND_MESSAGE, AggregatorReportSender.class)
        .build();

    return Configurations.merge(aggregatorConfiguration, contextConfiguration);
  }

  private Configuration getNameResolverServiceConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerPort.class, Integer.toString(nameServer.getPort()))
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
  }

  private void onActiveContext(final ActiveContext activeContext) {
    if (activeContext.getId().startsWith(AGGREGATOR_CONTEXT_PREFIX)) {
      LOG.log(Level.INFO, "VortexAggregator up and running");
      runtimeMaster.aggregatorAllocated(new VortexAggregatorManager(masterToAggregatorRequestor, activeContext));
      if (numLaunchedAggregators.incrementAndGet() == aggregatorNum) {
      }
    }
  }

  private void startThreads() {
    final ExecutorService pendingTaskSchedulerThread = Executors.newSingleThreadExecutor();
    pendingTaskSchedulerThread.execute(pendingTaskSchedulerRunner);
    pendingTaskSchedulerThread.shutdown();
    final ExecutorService userMainRunnerThread = Executors.newSingleThreadExecutor();
    userMainRunnerThread.execute(userMainRunner);
    userMainRunnerThread.shutdown();
  }

  public final class ActiveContextHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      onActiveContext(activeContext);
    }
  }

  public final class ContextMessageHandler implements EventHandler<ContextMessage> {
    @Override
    public void onNext(final ContextMessage contextMessage) {
      final VortexMessage vortexMessage = messageCodec.decode(contextMessage.get());

      // Invoke runtimeMaster methods
      runtimeMaster.aggregatorReported(vortexMessage);
    }
  }

  /**
   * Executoruator preempted.
   */
  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      // TODO: handle faults
      throw new RuntimeException(failedExecutoruator.getExecutoruatorException());
    }
  }

  public final class DriverStopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      runtimeMaster.applicationFinished();
    }
  }
}
