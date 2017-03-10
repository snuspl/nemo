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
package edu.snu.vortex.compiler.frontend.beam.operator;

import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.Operator;

import java.util.List;

/**
 * Windowing operator implementation.
 * This operator simply windows the given elements into finite windows according to a user-specified WindowFn.
 * As this functionality is unnecessary for batch processing workloads and for Vortex Runtime, this is left as below.
 * TODO #36: This class is to be updated with stream processing.
 */
public class WindowFn implements Operator {
  private final org.apache.beam.sdk.transforms.windowing.WindowFn windowFn;
  private OutputCollector outputCollector;

  public WindowFn(final org.apache.beam.sdk.transforms.windowing.WindowFn windowFn) {
    this.windowFn = windowFn;
  }

  @Override
  public void prepare(final OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void onData(final List data, final int from) {
    outputCollector.emit(0, data);
  }

  @Override
  public void close() {
  }
}
