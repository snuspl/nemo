package edu.snu.onyx.compiler.frontend.spark;

import org.apache.spark.SparkConf;

/**
 * Spark context.
 */
public final class SparkContext {
  private final SparkConf conf;

  /**
   * Constructor.
   * @param conf configuration of the context.
   */
  private SparkContext(final SparkConf conf) {
    this.conf = conf;
  }

  /**
   * Configuration.
   * @return the spark configuration.
   */
  public SparkConf conf() {
    return this.conf;
  }

  /**
   * Get or create a Spark Context.
   * @param conf configuration for the context.
   * @return the new spark context.
   */
  public static SparkContext getOrCreate(final SparkConf conf) {
    return new SparkContext(conf);
  }
}