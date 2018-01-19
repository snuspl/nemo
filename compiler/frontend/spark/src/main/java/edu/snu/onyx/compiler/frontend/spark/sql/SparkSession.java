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
package edu.snu.onyx.compiler.frontend.spark.sql;

import edu.snu.onyx.compiler.frontend.spark.core.SparkContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import scala.Option;
import scala.Tuple2;

import java.util.HashMap;
import java.util.UUID;

/**
 * A simple version of the Spark session, containing SparkContext that contains SparkConf.
 */
public final class SparkSession extends org.apache.spark.sql.SparkSession {
  /**
   * Constructor.
   * @param sparkContext the spark context for the session.
   */
  private SparkSession(final SparkContext sparkContext) {
    super(sparkContext);
  }

  @Override
  public DataFrameReader read() {
    return new DataFrameReader(this);
  }

  @Override
  public SparkContext sparkContext() {
    return (SparkContext) super.sparkContext();
  }

  /**
   * Get a builder for the session.
   * @return the session builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Spark Session Builder.
   */
  public static final class Builder extends org.apache.spark.sql.SparkSession.Builder {
    private final HashMap<String, String> options = new HashMap<>();

    @Override
    public Builder appName(final String name) {
      return config("spark.app.name", name);
    }

    @Override
    public Builder config(final SparkConf conf) {
      for (Tuple2<String, String> entry: conf.getAll()) {
        this.options.put(entry._1(), entry._2());
      }
      return this;
    }

    @Override
    public Builder config(final String key, final String value) {
      this.options.put(key, value);
      return this;
    }

    @Override
    public Builder master(final String master) {
      return config("spark.master", master);
    }

    @Override
    public SparkSession getOrCreate() {
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("onyx_user"));

      if (!options.containsKey("spark.master")) {
        return master("local").getOrCreate();
      }

      final Option<org.apache.spark.sql.SparkSession> activeSession = getActiveSession();
      if (activeSession.isDefined() && activeSession.get() != null) {
        final SparkSession session = (SparkSession) activeSession.get();
        options.forEach((k, v) -> session.sessionState().conf().setConfString(k, v));
        return session;
      }

      synchronized (SparkSession.class) {
        final Option<org.apache.spark.sql.SparkSession> defaultSession = getDefaultSession();
        if (defaultSession.isDefined() && defaultSession.get() != null) {
          final SparkSession session = (SparkSession) defaultSession.get();
          options.forEach((k, v) -> session.sessionState().conf().setConfString(k, v));
          return session;
        }

        final String randomAppName = UUID.randomUUID().toString();
        final SparkConf sparkConf = new SparkConf();
        options.forEach(sparkConf::set);
        if (!sparkConf.contains("spark.app.name")) {
          sparkConf.setAppName(randomAppName);
        }
        final SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
        options.forEach(sparkContext.conf()::set);
        if (!sparkContext.conf().contains("spark.app.name")) {
          sparkContext.conf().setAppName(randomAppName);
        }

        final SparkSession session = new SparkSession(sparkContext);
        options.forEach(session.sessionState().conf()::setConfString);
        setDefaultSession(session);

        return session;
      }
    }
  }
}
