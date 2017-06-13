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
package edu.snu.vortex.compiler.frontend.beam;

import edu.snu.vortex.runtime.common.state.JobState;
import edu.snu.vortex.runtime.master.JobStateManager;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.beam.sdk.PipelineResult.State.*;

/**
 * Beam result.
 * TODO #32: Implement Beam Result
 */
public final class Result implements PipelineResult {

  /**
   * The {@link JobStateManager} of the running pipeline.
   */
  private Optional<JobStateManager> jobStateManager;

  /**
   * A lock and condition to check whether the {@link JobStateManager} is set or not.
   */
  private final Lock jsmLock;
  private final Condition notSet;
  private static final long DEFAULT_JSM_WAIT_IN_MILLIS = 100;

  /**
   * Construct a Beam result.
   */
  public Result() {
    this.jobStateManager = Optional.empty();
    this.jsmLock = new ReentrantLock();
    this.notSet = jsmLock.newCondition();
  }

  /**
   * Set the job state manager.
   * @param jobStateManager the job state manager of the running pipeline.
   */
  public void setJobStateManager(final JobStateManager jobStateManager) {
    jsmLock.lock();
    try {
      this.jobStateManager = Optional.of(jobStateManager);
      notSet.signalAll();
    } finally {
      jsmLock.unlock();
    }
  }

  /**
   * Translate a job state of vortex to a corresponding Beam state.
   * @param jobState to translate.
   * @return the translated state.
   */
  private State translateState(final JobState jobState) {
    switch ((JobState.State) jobState.getStateMachine().getCurrentState()) {
      case READY:
        return UNKNOWN;
      case EXECUTING:
        return RUNNING;
      case COMPLETE:
        return DONE;
      case FAILED:
        return FAILED;
      default:
        throw new UnsupportedOperationException("Unsupported job state.");
    }
  }

  /**
   * Wait until the {@link JobStateManager} is set.
   * It wait for at most the given time.
   * @param timeoutInMillis of waiting.
   * @return {@code true} if the manager set.
   */
  private boolean waitUntilJsmSet(final long timeoutInMillis) {
    jsmLock.lock();
    try {
      if (!jobStateManager.isPresent()) {
        // If the state manager is not set yet, wait.
        return notSet.await(timeoutInMillis, TimeUnit.MILLISECONDS);
      } else {
        return true;
      }
    } catch (final InterruptedException e) {
      e.printStackTrace();
      return false;
    } finally {
      jsmLock.unlock();
    }
  }

  /**
   * Get the current state of the running pipeline.
   * It may wait for at most the default timeout.
   * @return the current pipeline state.
   */
  @Override
  public State getState() {
    if (waitUntilJsmSet(DEFAULT_JSM_WAIT_IN_MILLIS)) {
      return translateState(jobStateManager.get().getJobState());
    } else {
      return UNKNOWN;
    }
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("cancel() in frontend.beam.Result");
  }

  @Override
  public State waitUntilFinish(final Duration duration) {
    final long currentNano = System.nanoTime();
    if (waitUntilJsmSet(duration.getMillis())) {
      // Wait for the jsm to be set and subtract it from the given duration.
      final long consumedInMillis = (System.nanoTime() - currentNano) / 1000;
      return translateState(jobStateManager.get().
          waitUntilFinish(duration.getMillis() - consumedInMillis, TimeUnit.MILLISECONDS));
    } else {
      return UNKNOWN;
    }
  }

  @Override
  public State waitUntilFinish() {
    if (waitUntilJsmSet(DEFAULT_JSM_WAIT_IN_MILLIS)) {
      return translateState(jobStateManager.get().waitUntilFinish());
    } else {
      return UNKNOWN;
    }
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("metrics() in frontend.beam.Result");
  }
}
