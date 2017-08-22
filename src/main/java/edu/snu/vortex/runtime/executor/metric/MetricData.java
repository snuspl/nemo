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
package edu.snu.vortex.runtime.executor.metric;

import java.util.*;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * MetricData to collect executor side metrics.
 */
public class MetricData {

  private final Enum levelEnum;
  private final String levelId;
  private final String executorId;
  private final List<Integer> stageScheduleAttemptIdx;
  private final Map<String, String> stateChange;
  private final Map<String, Long> timestamp;

  public MetricData(final Enum level,
                    final String levelId,
                    final String executorId,
                    final List<Integer> stageScheduleAttempt,
                    final Map<String, String> stateChange,
                    final Map<String, Long> timestamp) {
    this.levelEnum = level;
    this.levelId = levelId;
    this.executorId = executorId;
    this.stageScheduleAttemptIdx = stageScheduleAttempt;
    this.stateChange = stateChange;
    this.timestamp = timestamp;
  }

  public final Enum getLevel() {
    return levelEnum;
  }

  public final String getLevelId() {
    return levelId;
  }

  public final String getExecutorId() {
    return executorId;
  }

  public final void setStageScheduleAttemptIdx(final Integer stageScheduleAttemptIdx) {
    this.stageScheduleAttemptIdx.set(0, stageScheduleAttemptIdx);
  }

  public final int getStageScheduleAttemptIdx() {
    return stageScheduleAttemptIdx.get(0);
  }

  public final void setStartState(final String startState) {
    this.stateChange.put("StartState", startState);
  }

  public final String getStartState() {
    return stateChange.get("StartState");
  }

  public final void setEndState(final String endState) {
    stateChange.put("EndState", endState);
  }

  public final String getEndState() {
    return stateChange.get("EndState");
  }

  public final void setStartTime(final Long startTime) {
    timestamp.put("StartTime", startTime);
  }

  public final long getStartTime() {
    return timestamp.get("StartTime");
  }

  public final void setEndTime(final Long endTime) {
    timestamp.put("EndTime", endTime);
  }

  public final long getEndTime() {
    return timestamp.get("EndTime");
  }

  /**
   * MetricData levels.
   */
  public enum Level {
    JOB,
    STAGE,
    TASKGROUP,
    TASK
  }

  @JsonValue
  public final Map<String, Object> toJson() {
    final Map<String, Object> jsonMetricData = new HashMap<>();
    Long elapsedTime = getEndTime() - getStartTime();
    jsonMetricData.put(getLevel().toString(), getLevelId());
    jsonMetricData.put("Executor", getExecutorId());
    jsonMetricData.put("StageScheduleAttemptIdx", getStageScheduleAttemptIdx());
    jsonMetricData.put("StartState", getStartState());
    jsonMetricData.put("EndState", getEndState());
    jsonMetricData.put("ElapsedTime", elapsedTime.toString());

    return jsonMetricData;
  }
}
