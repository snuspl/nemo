package edu.snu.onyx.compiler.frontend.spark.transform;

import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;

public final class ReduceTransform<T> implements Transform<T, T> {
  private final BinaryOperator<T> func;
  private OutputCollector<T> outputCollector;
  private final List<T> result;

  public ReduceTransform(final BinaryOperator<T> func, final List<T> result) {
    this.func = func;
    this.result = result;
  }

  @Override
  public void prepare(Context context, OutputCollector<T> outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void onData(Iterable<T> elements, String srcVertexId) {
    final List<T> list = new ArrayList<>();
    elements.forEach(list::add);
    final T result = list.stream().reduce(func)
        .orElseThrow(() -> new RuntimeException("Something wrong with the provided reduce operator"));
    this.result.add(result);
    outputCollector.emit(result);
  }

  @Override
  public void close() {
  }
}
