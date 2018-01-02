package edu.snu.onyx.compiler.frontend.spark.transform;

import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;

import java.io.Serializable;

public final class MapTransform<I extends Serializable, O extends Serializable> implements Transform<I, O> {
  private final SerializableFunction<I, O> func;
  private OutputCollector<O> outputCollector;

  public MapTransform(final SerializableFunction<I, O> func) {
    this.func = func;
  }

  @Override
  public void prepare(Context context, OutputCollector<O> outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void onData(Iterable<I> elements, String srcVertexId) {
    elements.forEach(element -> outputCollector.emit(func.apply(element)));
  }

  @Override
  public void close() {
  }
}
