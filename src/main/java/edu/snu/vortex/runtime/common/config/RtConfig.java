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
package edu.snu.vortex.runtime.common.config;

/**
 * RtConfig.
 */
public class RtConfig {
  private final RtExecMode rtExecMode;
  public static final String MASTER_NAME = "runtime_master";

  public static final int DEFAULT_EXECUTOR_NUM_CORES = 4;
  public static final int DEFAULT_EXECUTOR_CAPACITY = 10;

  public RtConfig(final RtExecMode rtExecMode) {
    this.rtExecMode = rtExecMode;
  }

  public final RtExecMode getRtExecMode() {
    return rtExecMode;
  }

  /**
   * RtExecMode.
   */
  public enum RtExecMode { STREAM, BATCH }
}