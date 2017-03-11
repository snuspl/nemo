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
import edu.snu.vortex.compiler.ir.Transform;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class PartitionKV implements Transform {
  private OutputCollector outputCollector;

  @Override
  public void prepare(final OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void onData(final List data, final int from) {
    final int numOfDsts = outputCollector.getDestinations().size();
    final List<List<KV>> dsts = new ArrayList<>(numOfDsts);
    IntStream.range(0, numOfDsts).forEach(x -> dsts.add(new ArrayList<>()));
    data.forEach(element -> {
      final KV kv = (KV)element;
      final int dstIndex = Math.abs(kv.getKey().hashCode() % numOfDsts);
      dsts.get(dstIndex).add(kv);
    });
    IntStream.range(0, numOfDsts).forEach(dstIndex -> outputCollector.emit(dstIndex, dsts.get(dstIndex)));
  }

  @Override
  public void close() {
    // do nothing
  }
}
