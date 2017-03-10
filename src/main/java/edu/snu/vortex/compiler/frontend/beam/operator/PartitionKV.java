package edu.snu.vortex.compiler.frontend.beam.operator;

import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.Operator;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class PartitionKV implements Operator {
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
