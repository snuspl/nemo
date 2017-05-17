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
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Driver for Vortex.
 */
@Unit
@DriverSide
final class VortexDriver {
  private static final Logger LOG = Logger.getLogger(VortexDriver.class.getName());
  private static final String AGGREGATOR_CONTEXT_PREFIX = "AGGREGATOR_CONTEXT_";

  private final AtomicInteger numberOfFailures = new AtomicInteger(0);
  private final EvaluatorRequestor evaluatorRequestor; // for requesting resources
  private final VortexMaster vortexMaster; // Vortex VortexMaster
  private final MasterToExecutorRequestor masterToExecutorRequestor; // For sending Commands to remote executors
  private final MasterToAggregatorRequestor masterToAggregatorRequestor;
  private final VortexMessageCodec messageCodec;
  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;

  // Resource configuration for single thread pool
  private final int evalMem;
  private final int evalNum;
  private final int evalCores;

  private final int executorNum;
  private final int executorCores;
  private final int executorCapacity;
  private final int aggregatorNum;
  private final int aggregatorCores;

  private final int maxFailures;

  private final UserMainRunner userMainRunner;
  private final PendingTaskSchedulerRunner pendingTaskSchedulerRunner;

  @Inject
  private VortexDriver(final EvaluatorRequestor evaluatorRequestor,
                       final MasterToExecutorRequestor masterToExecutorRequestor,
                       final MasterToAggregatorRequestor masterToAggregatorRequestor,
                       final VortexMessageCodec messageCodec,
                       final VortexMaster vortexMaster,
                       final UserMainRunner userMainRunner,
                       final PendingTaskSchedulerRunner pendingTaskSchedulerRunner,
                       final NameServer nameServer,
                       final LocalAddressProvider localAddressProvider,
                       @Parameter(JobConf.EvalMem.class) final int evalMem,
                       @Parameter(JobConf.EvalNum.class) final int evalNum,
                       @Parameter(JobConf.EvalCores.class) final int evalCores,
                       @Parameter(JobConf.ExecutorNum.class) final int executorNum,
                       @Parameter(JobConf.ExecutorThreads.class) final int executorCores,
                       @Parameter(JobConf.ExecutorCapacity.class) final int executorCapacity,
                       @Parameter(JobConf.AggregatorNum.class) final int aggregatorNum,
                       @Parameter(JobConf.AggregatorThreads.class) final int aggregatorCores,
                       @Parameter(JobConf.MaxFailures.class) final int maxFailures) {
    this.userMainRunner = userMainRunner;
    this.pendingTaskSchedulerRunner = pendingTaskSchedulerRunner;
    this.evaluatorRequestor = evaluatorRequestor;
    this.messageCodec = messageCodec;
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.vortexMaster = vortexMaster;
    this.masterToExecutorRequestor = masterToExecutorRequestor;
    this.masterToAggregatorRequestor = masterToAggregatorRequestor;
    this.evalMem = evalMem;
    this.evalNum = evalNum;
    this.evalCores = evalCores;
    this.executorCapacity = executorCapacity;
    this.executorNum = executorNum;
    this.executorCores = executorCores;
    this.aggregatorNum = aggregatorNum;
    this.aggregatorCores = aggregatorCores;
    this.maxFailures = maxFailures;
  }

  /**
   * Driver started.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // Initial Evaluator Request
      startJob();
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(evalNum)
          .setMemory(evalMem)
          .setNumberOfCores(evalCores)
          .build());
    }
  }

  /**
   * Container allocated.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
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
      vortexMaster.aggregatorAllocated(new VortexAggregatorManager(masterToAggregatorRequestor, activeContext));
      if (numLaunchedAggregators.incrementAndGet() == aggregatorNum) {
      }
    }
  }

  private void startJob() {
    final ExecutorService pendingTaskSchedulerThread = Executors.newSingleThreadExecutor();
    pendingTaskSchedulerThread.execute(pendingTaskSchedulerRunner);
    pendingTaskSchedulerThread.shutdown();
    final ExecutorService userMainRunnerThread = Executors.newSingleThreadExecutor();
    userMainRunnerThread.execute(userMainRunner);
    userMainRunnerThread.shutdown();
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      onActiveContext(activeContext);
    }
  }

  final class ContextMessageHandler implements EventHandler<ContextMessage> {
    @Override
    public void onNext(final ContextMessage contextMessage) {
      final VortexMessage vortexMessage = messageCodec.decode(contextMessage.get());
      vortexMaster.aggregatorReported(vortexMessage);
    }
  }

  /**
   * Evaluator preempted.
   */
  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      // TODO: handle faults
      throw new RuntimeException(failedEvaluator.getEvaluatorException());
    }
  }

  final class DriverStopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      vortexMaster.applicationFinished();
    }
  }
}
