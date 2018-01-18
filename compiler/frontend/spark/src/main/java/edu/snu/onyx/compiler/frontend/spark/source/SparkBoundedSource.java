package edu.snu.onyx.compiler.frontend.spark.source;

import edu.snu.onyx.common.ir.vertex.Source;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Spark bounded source.
 * @param <T> type of the data to read.
 */
public final class SparkBoundedSource<T> implements Source<T> {
  private final String filePath;

  /**
   * Constructor.
   * @param filePath the path to read from.
   */
  public SparkBoundedSource(final String filePath) {
    this.filePath = filePath;
  }

  @Override
  public List<? extends Source<T>> split(final long var1) throws Exception {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public long getEstimatedSizeBytes() throws Exception {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Source.Reader<T> createReader() throws IOException {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  /**
   * Spark bounded reader.
   * @param <T> type of the data to read.
   */
  class SparkBoundedReader<T> implements Source.Reader<T> {

    @Override
    public boolean start() throws IOException {
      throw new UnsupportedOperationException("Operation not yet implemented.");
    }

    @Override
    public boolean advance() throws IOException {
      throw new UnsupportedOperationException("Operation not yet implemented.");
    }

    @Override
    public void close() throws IOException {
      throw new UnsupportedOperationException("Operation not yet implemented.");
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      throw new UnsupportedOperationException("Operation not yet implemented.");
    }

    @Override
    public Source<T> getCurrentSource() {
      throw new UnsupportedOperationException("Operation not yet implemented.");
    }
  }
}
