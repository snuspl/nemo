package edu.snu.onyx.compiler.frontend.spark.source;

import edu.snu.onyx.common.ir.Reader;
import edu.snu.onyx.common.ir.vertex.SourceVertex;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Bounded source vertex for Spark.
 * @param <T> type of data to read.
 */
public final class SparkBoundedSourceVertex<T> extends SourceVertex<T> {
  private final RDD<T> rdd;

  /**
   * Constructor.
   * @param rdd RDD to read data from.
   */
  public SparkBoundedSourceVertex(final RDD<T> rdd) {
    this.rdd = rdd;
  }

  @Override
  public SparkBoundedSourceVertex getClone() {
    final SparkBoundedSourceVertex<T> that = new SparkBoundedSourceVertex<>(this.rdd);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Reader<T>> getReaders(final int desiredNumOfSplits) throws Exception {
    // desiredNumOfSplits is ignored. RDD configures the number.
    final List<Reader<T>> readers = new ArrayList<>();
    for (int i = 0; i < rdd.getNumPartitions(); i++) {
      readers.add(new SparkBoundedSourceReader<>(rdd, i));
    }
    return readers;
  }

  /**
   * SparkBoundedSourceReader class.
   * @param <T> type of data.
   */
  public class SparkBoundedSourceReader<T> implements Reader<T> {
    private int index;
    private final RDD<T> rdd;

    /**
     * Constructor of SparkBoundedSourceReader.
     * @param rdd the RDD to load data from.
     * @param index index of the partition.
     */
    SparkBoundedSourceReader(final RDD<T> rdd, final int index) {
      this.rdd = rdd;
      this.index = index;
    }

    @Override
    public final Iterator<T> read() {
      return rdd.toJavaRDD().collectPartitions(new int[]{index})[0].iterator();
    }
  }
}
