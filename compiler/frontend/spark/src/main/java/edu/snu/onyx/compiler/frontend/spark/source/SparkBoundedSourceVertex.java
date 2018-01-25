package edu.snu.onyx.compiler.frontend.spark.source;

import edu.snu.onyx.common.ir.Readable;
import edu.snu.onyx.common.ir.ReadablesWrapper;
import edu.snu.onyx.common.ir.vertex.SourceVertex;
import edu.snu.onyx.compiler.frontend.spark.sql.Dataset;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext$;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Bounded source vertex for Spark.
 * @param <T> type of data to read.
 */
public final class SparkBoundedSourceVertex<T> extends SourceVertex<T> {
  private final List<Iterator<T>> partitionedData;

  /**
   * Constructor.
   * Note that we have to first create our iterators here and supply them to our readables.
   * @param dataset Dataset to read data from.
   */
  public SparkBoundedSourceVertex(final Dataset<T> dataset) {
    this.partitionedData = new ArrayList<>();
    for (final Partition partition : dataset.rdd().getPartitions()) {
      final Iterator<T> data = JavaConverters
          .asJavaIteratorConverter(dataset.rdd().compute(partition, TaskContext$.MODULE$.empty())).asJava();
      partitionedData.add(data);
    }
  }

  public SparkBoundedSourceVertex(final List<Iterator<T>> partitionedData) {
    this.partitionedData = partitionedData;
  }

  @Override
  public SparkBoundedSourceVertex getClone() {
    final SparkBoundedSourceVertex<T> that = new SparkBoundedSourceVertex<>((this.partitionedData));
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public ReadablesWrapper<T> getReadableWrapper(final int desiredNumOfSplits) {
    return new SparkBoundedSourceReadablesWrapper();
  }

  /**
   * A ReadablesWrapper for SparkBoundedSourceVertex.
   */
  private final class SparkBoundedSourceReadablesWrapper implements ReadablesWrapper<T> {
    /**
     * Constructor.
     */
    private SparkBoundedSourceReadablesWrapper() {
    }

    @Override
    public List<Readable<T>> getReadables() {
      final List<Readable<T>> readables = new ArrayList<>();
      for (int i = 0; i < partitionedData.size(); i++) {
        readables.add(new BoundedSourceReadable(i));
      }
      return readables;
    }
  }

  /**
   * A Readable for SparkBoundedSourceReadablesWrapper.
   */
  private final class BoundedSourceReadable implements Readable<T> {
    final Integer index;

    /**
     * Constructor.
     * @param index
     */
    private BoundedSourceReadable(final Integer index) {
      this.index = index;
    }

    @Override
    public Iterable<T> read() {
      final List<T> data = new ArrayList<>();
      partitionedData.get(index).forEachRemaining(data::add);
      return data;
    }
  }
}
