package edu.snu.vortex.compiler.frontend.beam.udf;

import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.UserDefinedFunction;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class PartitionKV implements UserDefinedFunction {
  private OutputCollector outputCollector;

  @Override
  public void prepare(final OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void onData(final List data, final int from) {
    final int numOfDsts = outputCollector.getDestinations().size();
    final List<List<WindowedValue<KV>>> dsts = new ArrayList<>(numOfDsts);
    IntStream.range(0, numOfDsts).forEach(x -> dsts.add(new ArrayList<>()));
    data.forEach(input -> {
      final WindowedValue<KV> windowedValue = (WindowedValue<KV>)input;
      final KV kv = windowedValue.getValue();
      final int dstIndex = Math.abs(kv.getKey().hashCode() % numOfDsts);
      dsts.get(dstIndex).add(windowedValue);
    });
    IntStream.range(0, numOfDsts).forEach(dstIndex -> outputCollector.emit(dstIndex, dsts.get(dstIndex)));
  }

  @Override
  public void close() {
  }
}
