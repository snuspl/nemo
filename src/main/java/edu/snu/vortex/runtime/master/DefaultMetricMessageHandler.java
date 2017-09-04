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

import javax.inject.Inject;
import java.util.Map;

import edu.snu.vortex.runtime.common.metric.MetricDataBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A default metric message handler.
 */
public final class DefaultMetricMessageHandler implements MetricMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricMessageHandler.class.getName());

  @Inject
  public DefaultMetricMessageHandler() {

  }

  public void startPoint(final Enum computationUnitEnum,
                         final String computationUnitId,
                         final String computationUnitKey,
                         final String executorId,
                         final int attemptIdx,
                         final Enum state,
                         final Map<String, MetricDataBuilder> metricDataBuilderMap) {
    final MetricDataBuilder metricDataBuilder = new MetricDataBuilder(computationUnitEnum,
                                                                      computationUnitId, executorId);
    metricDataBuilder.beginMeasurement(attemptIdx, state, System.nanoTime());
    metricDataBuilderMap.put(computationUnitKey, metricDataBuilder);
  }

  public void endPoint(final String computationUnitKey,
                       final Enum state,
                       final Map<String, MetricDataBuilder> metricDataBuilderMap) {
    final MetricDataBuilder metricDataBuilder = metricDataBuilderMap.get(computationUnitKey);
    metricDataBuilder.endMeasurement(state, System.nanoTime());
    onMetricMessageReceived(metricDataBuilder.build().toJson());
    metricDataBuilderMap.remove(computationUnitKey);
  }

  @Override
  public void onMetricMessageReceived(final String jsonStr) {
    LOG.debug("{}", jsonStr);
  }
}
