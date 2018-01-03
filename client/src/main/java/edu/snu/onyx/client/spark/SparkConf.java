package edu.snu.onyx.client.spark;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Spark configurations.
 */
public final class SparkConf {
  private final ConcurrentHashMap<String, String> settings;

  /**
   * Default constructor.
   */
  SparkConf() {
    this.settings = new ConcurrentHashMap<>();
  }

  /**
   * Set application name.
   * @param name name of the application.
   */
  public void setAppName(final String name) {
    set("spark.app.name", name);
  }

  /**
   * Set a configuration.
   * @param k key of the configuration.
   * @param v value of the configuration.
   */
  public void set(final String k, final String v) {
    settings.put(k, v);
  }
}
