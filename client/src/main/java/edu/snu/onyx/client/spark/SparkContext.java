package edu.snu.onyx.client.spark;

public final class SparkContext {
  final public SparkConf conf;

  private SparkContext(final SparkConf conf) {
    this.conf = conf;
  }

  public static SparkContext getOrCreate(final SparkConf conf) {
    return new SparkContext(conf);
  }
}
