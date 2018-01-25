package edu.snu.onyx.compiler.frontend.spark.source;

import edu.snu.onyx.common.ir.Readable;
import edu.snu.onyx.common.ir.ReadablesWrapper;
import edu.snu.onyx.common.ir.vertex.SourceVertex;
import edu.snu.onyx.compiler.frontend.spark.sql.Dataset;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext$;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;

/**
 * Bounded source vertex for Spark.
 * @param <T> type of data to read.
 */
public final class SparkBoundedSourceVertex<T> extends SourceVertex<T> {
  private final ReadablesWrapper<T> readablesWrapper;

  /**
   * Constructor.
   * Note that we have to first create our iterators here and supply them to our readables.
   * @param dataset Dataset to read data from.
   */
  public SparkBoundedSourceVertex(final Dataset<T> dataset) {
    this.readablesWrapper = new SparkBoundedSourceReadablesWrapper(dataset);
  }

  public SparkBoundedSourceVertex(final ReadablesWrapper<T> readablesWrapper) {
    this.readablesWrapper = readablesWrapper;
  }

  @Override
  public SparkBoundedSourceVertex getClone() {
    final SparkBoundedSourceVertex<T> that = new SparkBoundedSourceVertex<>((this.readablesWrapper));
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public ReadablesWrapper<T> getReadableWrapper(final int desiredNumOfSplits) {
    return readablesWrapper;
  }

  /**
   * A ReadablesWrapper for SparkBoundedSourceVertex.
   */
  private final class SparkBoundedSourceReadablesWrapper implements ReadablesWrapper<T> {
    private final List<Readable<T>> readables;

    /**
     * Constructor.
     */
    private SparkBoundedSourceReadablesWrapper(final Dataset<T> dataset) {
      this.readables = new ArrayList<>();
      for (final Partition partition: dataset.rdd().getPartitions()) {
        readables.add(new BoundedSourceReadable(dataset.rdd().sparkContext().conf(), partition, dataset.rdd()));
      }
    }

    @Override
    public List<Readable<T>> getReadables() {
      return readables;
    }
  }

  /**
   * A Readable for SparkBoundedSourceReadablesWrapper.
   */
  private final class BoundedSourceReadable implements Readable<T> {
    final List<T> data;

    /**
     * Constructor.
     */
    private BoundedSourceReadable(final SparkConf conf, final Partition partition, final RDD<T> rdd) {
      this.data = new ArrayList<>();
      JavaConverters.asJavaIteratorConverter(rdd.compute(partition, TaskContext$.MODULE$.empty())).asJava()
          .forEachRemaining(data::add);
    }

    @Override
    public Iterable<T> read() {
      return data;
    }
  }
}
