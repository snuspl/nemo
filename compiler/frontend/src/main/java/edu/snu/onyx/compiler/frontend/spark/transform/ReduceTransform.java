package edu.snu.onyx.compiler.frontend.spark.transform;

import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Reduce Transform for Spark.
 * @param <T> element type.
 */
public final class ReduceTransform<T extends Serializable> implements Transform<T, T> {
  private final SerializableBinaryOperator<T> func;
  private OutputCollector<T> oc;
  private final List<T> result;

  /**
   * Constructor.
   * @param func function to run for the reduce transform.
   * @param result list to keep the result in.
   */
  public ReduceTransform(final SerializableBinaryOperator<T> func, final List<T> result) {
    this.func = func;
    this.result = result;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> outputCollector) {
    this.oc = outputCollector;
  }

  @Override
  public void onData(final Iterable<T> elements, final String srcVertexId) {
    final List<T> list = new ArrayList<>();
    elements.forEach(list::add);
    final T res = list.stream().reduce(func)
        .orElseThrow(() -> new RuntimeException("Something wrong with the provided reduce operator"));
    this.result.add(res);
    oc.emit(res);
  }

  @Override
  public void close() {
  }
}
