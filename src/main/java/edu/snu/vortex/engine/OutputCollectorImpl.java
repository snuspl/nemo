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
package edu.snu.vortex.engine;

import edu.snu.vortex.compiler.ir.OutputCollector;

import java.util.HashMap;
import java.util.List;

/**
 * Output Collector Implementation.
 */
public final class OutputCollectorImpl implements OutputCollector {
  private final HashMap<Integer, List> outputs;
  private final int numOfOutputs;

  public OutputCollectorImpl(final int numOfOutputs) {
    this.numOfOutputs = numOfOutputs;
    this.outputs = new HashMap<>();
  }

  public HashMap<Integer, List> getOutputs() {
    return outputs;
  }

  @Override
  public List<Integer> getDestinations() {
    return null;
  }

  @Override
  public void emit(final int index, final List output) {
    if (index >= numOfOutputs || index < 0) {
      throw new IllegalArgumentException("Index out of bounds");
    }

    if (outputs.containsKey(index)) {
      throw new IllegalArgumentException("Can not overwrite output");
    }

    outputs.put(index, output);
  }
}
