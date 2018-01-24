package edu.snu.onyx.compiler.frontend.spark.source;

import edu.snu.onyx.common.ir.Reader;
import edu.snu.onyx.common.ir.vertex.SourceVertex;

import java.util.*;

/**
 * Bounded source vertex for Spark.
 * @param <T> type of data to read.
 */
public final class SparkBoundedSourceVertex<T> extends SourceVertex<T> {
  private final List<Reader<T>> readers;

  /**
   * Constructor.
   * @param dataset set of data.
   */
  public SparkBoundedSourceVertex(final List<Iterator<T>> dataset) {
    this.readers = new ArrayList<>();
    dataset.forEach(data -> {
      final List<T> dataForReader = new ArrayList<>();
      data.forEachRemaining(dataForReader::add);
      readers.add(new SparkBoundedSourceReader<>(dataForReader));
    });
  }

  public SparkBoundedSourceVertex(final Set<Reader<T>> readers) {
    this.readers = new ArrayList<>();
    this.readers.addAll(readers);
  }

  @Override
  public SparkBoundedSourceVertex getClone() {
    final SparkBoundedSourceVertex<T> that = new SparkBoundedSourceVertex<>(new HashSet<>(this.readers));
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Reader<T>> getReaders(final int desiredNumOfSplits) throws Exception {
    return this.readers;
  }

  /**
   * SparkBoundedSourceReader class.
   * @param <T> type of data.
   */
  public class SparkBoundedSourceReader<T> implements Reader<T> {
    private final List<T> data;

    /**
     * Constructor of SparkBoundedSourceReader.
     * @param data List of data.
     */
    SparkBoundedSourceReader(final List<T> data) {
      this.data = data;
    }

    @Override
    public final Iterator<T> read() {
      return this.data.iterator();
    }
  }
}
