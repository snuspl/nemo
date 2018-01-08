package edu.snu.onyx.compiler.frontend.spark.transform;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
  private final String filename;

  /**
   * Constructor.
   * @param func function to run for the reduce transform.
   * @param filename file to keep the result in.
   */
  public ReduceTransform(final SerializableBinaryOperator<T> func, final String filename) {
    this.func = func;
    this.filename = filename;
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
    try {
      final Kryo kryo = new Kryo();
      final Output output = new Output(new FileOutputStream(filename));
      kryo.writeClassAndObject(output, res);
      output.close();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    oc.emit(res);
  }

  @Override
  public void close() {
  }
}
