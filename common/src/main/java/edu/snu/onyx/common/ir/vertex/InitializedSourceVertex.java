package edu.snu.onyx.common.ir.vertex;

import edu.snu.onyx.common.ir.Reader;

import java.util.ArrayList;
import java.util.List;

/**
 * Source vertex with initial data.
 * @param <T> type of initial data.
 */
public final class InitializedSourceVertex<T> extends SourceVertex<T> {
  private final Iterable<T> initializedSourceData;

  /**
   * Constructor.
   * @param initializedSourceData the initial data.
   */
  public InitializedSourceVertex(final Iterable<T> initializedSourceData) {
    this.initializedSourceData = initializedSourceData;
  }

  @Override
  public InitializedSourceVertex<T> getClone() {
    final InitializedSourceVertex<T> that = new InitializedSourceVertex<>(this.initializedSourceData);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Reader<T>> getReaders(final int desiredNumOfSplits) throws Exception {
    final List<Reader<T>> readers = new ArrayList<>();
    for (int i = 0; i < desiredNumOfSplits; i++) {
      readers.add(new InitializedSourceReader<>(initializedSourceData));
    }
    return readers;
  }

  /**
   * Reader for initialized source vertex.
   * @param <T> type of the initial data.
   */
  public class InitializedSourceReader<T> implements Reader<T> {
    private final Iterable<T> initializedSourceData;

    /**
     * Constructor.
     * @param initializedSourceData the source data.
     */
    InitializedSourceReader(final Iterable<T> initializedSourceData) {
      this.initializedSourceData = initializedSourceData;
    }

    @Override
    public final Iterable<T> read() throws Exception {
      return this.initializedSourceData;
    }
  }
}
