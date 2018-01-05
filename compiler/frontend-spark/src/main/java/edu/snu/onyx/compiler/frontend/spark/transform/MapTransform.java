package edu.snu.onyx.compiler.frontend.spark.transform;

import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;

import java.io.Serializable;

/**
 * Map Transform for Spark.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class MapTransform<I extends Serializable, O extends Serializable> implements Transform<I, O> {
  private final SerializableFunction<I, O> func;
  private OutputCollector<O> oc;

  /**
   * Constructor.
   * @param func the function to run map with.
   */
  public MapTransform(final SerializableFunction<I, O> func) {
    this.func = func;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> outputCollector) {
    this.oc = outputCollector;
  }

  @Override
  public void onData(final Iterable<I> elements, final String srcVertexId) {
    elements.forEach(element -> oc.emit(func.apply(element)));
  }

  @Override
  public void close() {
  }
}
