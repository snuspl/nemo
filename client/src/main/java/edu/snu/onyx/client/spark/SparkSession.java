/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.onyx.client.spark;

import java.util.HashMap;

public final class SparkSession {
  final private HashMap<String, String> initialSessionOptions;
  final private SparkContext sparkContext;

  private SparkSession(final SparkContext sparkContext) {
    this.initialSessionOptions = new HashMap<>();
    this.sparkContext = sparkContext;
  }

  public SparkContext sparkContext() {
    return sparkContext;
  }

  public static SparkSessionBuilder builder() {
    return new SparkSessionBuilder();
  }

  public void stop() {
  }

  public final static class SparkSessionBuilder {
    final HashMap<String, String> options;

    private SparkSessionBuilder() {
      this.options = new HashMap<>();
    }

    public SparkSessionBuilder appName(final String name) {
      return config("spark.app.name", name);
    }

    SparkSessionBuilder config(final String key, final String value) {
      this.options.put(key, value);
      return this;
    }

    public SparkSession getOrCreate() {
      final SparkConf sparkConf = new SparkConf();
      final SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
      options.forEach(sparkContext.conf::set);
      if (!options.containsKey("spark.app.name")) {
        sparkContext.conf.setAppName("TODO: random app name");
      }

      final SparkSession session = new SparkSession(sparkContext);
      options.forEach(session.initialSessionOptions::put);

      return session;
    }
  }
}
