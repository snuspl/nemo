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
package edu.snu.coral.compiler.frontend.spark.sql;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple version of the Spark session, containing SparkContext that contains SparkConf.
 */
public final class SparkSession extends org.apache.spark.sql.SparkSession {
  private final Map<String, String> initialConf;

  /**
   * Constructor.
   * @param sparkContext the spark context for the session.
   * @param initialConf initial configuration.
   */
  private SparkSession(final SparkContext sparkContext, final Map<String, String> initialConf) {
    super(sparkContext);
    this.initialConf = initialConf;
  }

  /**
   * @return the initial configuration of the session.
   */
  public Map<String, String> getInitialConf() {
    return initialConf;
  }

  @Override
  public DataFrameReader read() {
    return new DataFrameReader(this);
  }

  /**
   * Method to downcast Spark's spark session to our spark session class.
   * @param sparkSession spark's spark session.
   * @param initialConf initial configuration of the spark session.
   * @return our spark session class.
   */
  public static SparkSession from(final org.apache.spark.sql.SparkSession sparkSession,
                                  final Map<String, String> initialConf) {
    return new SparkSession(sparkSession.sparkContext(), initialConf);
  }

  /**
   * Get a builder for the session.
   * @return the session builder.
   */
  public static Builder builder() {
    return new Builder().master("local");
  }

  /**
   * Spark Session Builder.
   */
  public static final class Builder extends org.apache.spark.sql.SparkSession.Builder {
    private final Map<String, String> options = new HashMap<>();

    @Override
    public Builder appName(final String name) {
      return (Builder) super.appName(name);
    }

    @Override
    public Builder config(final SparkConf conf) {
      for (Tuple2<String, String> kv : conf.getAll()) {
        this.options.put(kv._1, kv._2);
      }
      return (Builder) super.config(conf);
    }

    /**
     * Apply config in the form of Java Map.
     * @param conf the conf.
     * @return the builder with the conf applied.
     */
    public Builder config(final Map<String, String> conf) {
      conf.forEach((k, v) -> {
        this.options.put(k, v);
        super.config(k, v);
      });
      return this;
    }

    @Override
    public Builder config(final String key, final String value) {
      this.options.put(key, value);
      return (Builder) super.config(key, value);
    }

    @Override
    public Builder master(final String master) {
      return (Builder) super.master(master);
    }

    @Override
    public SparkSession getOrCreate() {
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("coral_user"));
      return SparkSession.from(super.getOrCreate(), this.options);
    }
  }
}
