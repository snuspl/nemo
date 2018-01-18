package edu.snu.onyx.compiler.frontend.spark.transform;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import edu.snu.onyx.compiler.frontend.spark.core.java.JavaRDD;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Iterator;

/**
 * Collect transform.
 * @param <T> type of data to collect.
 */
public final class CollectTransform<T> implements Transform<T, T> {
  private String filename;

  /**
   * Constructor.
   * @param filename file to keep the result in.
   */
  public CollectTransform(final String filename) {
    this.filename = filename;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> outputCollector) {
    this.filename = filename + JavaRDD.getResultId();
  }

  @Override
  public void onData(final Iterator<T> elements, final String srcVertexId) {
    // Write result to a temporary file.
    // TODO #740: remove this part, and make it properly transfer with executor.
    try {
      final Kryo kryo = new Kryo();
      final Output output = new Output(new FileOutputStream(filename));
      elements.forEachRemaining(element -> kryo.writeClassAndObject(output, element));
      output.close();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }
}
