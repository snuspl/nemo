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

import java.util.HashMap;
import java.util.List;

/**
 * Data context.
 */
public abstract class OperatorContext {
  final List input;
  final int inputIndex;
  final HashMap<Integer, List> outputs;
  final int numOfOutputs;

  public OperatorContext(final List input,
                         final int inputIndex,
                         final int numOfOutputs) {
    this.input = input;
    this.inputIndex = inputIndex;
    this.numOfOutputs = numOfOutputs;
    this.outputs = new HashMap<>();
  }

  public List getInput() {
    return input;
  }

  public int getInputIndex() {
    return inputIndex;
  }

  public void output(final int index, final List output) {
    if (index >= numOfOutputs || index < 0)
      throw new RuntimeException("Index out of bounds");

    if (outputs.containsKey(index))
      throw new RuntimeException("Can not overwrite output");

    outputs.put(index, output);
  }

  public HashMap<Integer, List> getOutputs() {
    return outputs;
  }

  abstract public List getBroadcastedData();
}
