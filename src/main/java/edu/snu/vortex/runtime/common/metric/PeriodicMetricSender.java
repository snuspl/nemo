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
package edu.snu.vortex.runtime.common.metric;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.exception.UnknownFailureCauseException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.common.metric.parameter.MetricFlushPeriod;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metric sender that periodically flushes the collected metrics to Driver.
 */
public final class PeriodicMetricSender implements MetricSender {

  private final ScheduledExecutorService scheduledExecutorService;
  private final BlockingQueue<String> metricMessageQueue;
  private final AtomicBoolean closed;

  private static final Logger LOG = LoggerFactory.getLogger(PeriodicMetricSender.class.getName());

  @Inject
  private PeriodicMetricSender(@Parameter(MetricFlushPeriod.class) final long flushingPeriod,
                               @Parameter(JobConf.ExecutorId.class) final String executorId,
                               final PersistentConnectionToMaster persistentConnectionToMaster) {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.metricMessageQueue = new LinkedBlockingQueue<>();
    this.closed = new AtomicBoolean(false);
    this.scheduledExecutorService.scheduleAtFixedRate(() -> {
      while (!closed.get() || !metricMessageQueue.isEmpty()) {
        final String metricMsg = metricMessageQueue.poll();
        final ControlMessage.MetricMsg.Builder metricMsgBuilder = ControlMessage.MetricMsg.newBuilder()
            .setMetricMessage(metricMsg);

        persistentConnectionToMaster.getMessageSender().send(
            ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setType(ControlMessage.MessageType.MetricMessageReceived)
            .setMetricMsg(metricMsgBuilder.build())
            .build());
      }
    }, 0, flushingPeriod, TimeUnit.MILLISECONDS);
  }

  @Override
  public void send(final String metricData) {
    metricMessageQueue.add(metricData);
  }

  @Override
  public void close() throws UnknownFailureCauseException {
    closed.set(true);
    scheduledExecutorService.shutdown();
  }
}
