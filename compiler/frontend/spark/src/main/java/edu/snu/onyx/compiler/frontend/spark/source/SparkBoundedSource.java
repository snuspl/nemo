package edu.snu.onyx.compiler.frontend.spark.source;

import edu.snu.onyx.common.ir.vertex.Source;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

public final class SparkBoundedSource<T> implements Source<T> {
  private final String filePath;

  public SparkBoundedSource(final String filePath) {
    this.filePath = filePath;
  }

  @Override
  public List<? extends Source<T>> split(long var1) throws Exception {

  }

  @Override
  public long getEstimatedSizeBytes() throws Exception {

  }

  @Override
  public Source.Reader<T> createReader() throws IOException {

  }

  class SparkBoundedReader<T> implements Source.Reader<T> {

    @Override
    public boolean start() throws IOException {

    }

    @Override
    public boolean advance() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public T getCurrent() throws NoSuchElementException {

    }

    @Override
    public Source<T> getCurrentSource() {

    }
  }
}
