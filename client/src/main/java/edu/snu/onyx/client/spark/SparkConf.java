package edu.snu.onyx.client.spark;

import java.util.concurrent.ConcurrentHashMap;

public class SparkConf {
  private final ConcurrentHashMap<String, String> settings;

  SparkConf() {
    this.settings = new ConcurrentHashMap<>();
  }

  public void setAppName(final String name) {
    set("spark.app.name", name);
  }

  public void set(final String k, final String v) {
    settings.put(k, v);
  }
}
