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
package edu.snu.nemo.compiler.frontend.spark.sql;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import javax.naming.OperationNotSupportedException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A simple version of the Spark session, containing SparkContext that contains SparkConf.
 */
public final class SparkSession extends org.apache.spark.sql.SparkSession implements NemoSparkSQL {
  private final LinkedHashMap<String, Object[]> datasetCommandsList;
  private final Map<String, String> initialConf;
  private boolean userTriggered;

  /**
   * Constructor.
   * @param sparkContext the spark context for the session.
   */
  private SparkSession(final SparkContext sparkContext, final Map<String, String> initialConf) {
    super(sparkContext);
    this.datasetCommandsList = new LinkedHashMap<>();
    this.initialConf = initialConf;
    this.userTriggered = true;
  }

  @Override
  public boolean isUserTriggered() {
    return userTriggered;
  }

  @Override
  public void setUserTriggered(final boolean userTriggered) {
    this.userTriggered = userTriggered;
  }

  /**
   *
   * @param session session to append the commands to.
   * @param obj object to call the superclass's method from.
   * @param args arguments of the method.
   * @return the result of the superclass method.
   */
  public static Object callSuperclassMethod(final SparkSession session,
                                            final NemoSparkSQL obj,
                                            final Object... args) {
    final String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
    final Class<?>[] argTypes = Stream.of(args).map(o -> o.getClass()).toArray(Class[]::new);
    final boolean isUserTriggered = obj.isUserTriggered();

    try {
      if (isUserTriggered) {
        session.appendCommand(obj.getClass().getSimpleName() + "#" + methodName, args);
        obj.setUserTriggered(false);
      }

      final Method method = obj.getClass().getSuperclass().getDeclaredMethod(methodName, argTypes);
      final Object result = method.invoke(obj, args);

      if (isUserTriggered) {
        obj.setUserTriggered(true);
      }

      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Append the command to the list of dataset commands.
   * @param cmd the name of the command to apply. e.g. "SparkSession#read"
   * @param args arguments required for the command.
   */
  private void appendCommand(final String cmd, final Object... args) {
    this.datasetCommandsList.put(cmd, args);
  }

  /**
   * @return the commands list required to recreate the dataset on separate machines.
   */
  public LinkedHashMap<String, Object[]> getDatasetCommandsList() {
    return this.datasetCommandsList;
  }

  /**
   * @return the initial configuration of the session.
   */
  public Map<String, String> getInitialConf() {
    return initialConf;
  }

  /**
   * Method to reproduce the initial Dataset on separate evaluators when reading from sources.
   * @param spark sparkSession to start from.
   * @param commandList commands required to setup the dataset.
   * @param <T> type of the resulting dataset's data.
   * @return the initialized dataset.
   * @throws OperationNotSupportedException exception when the command is not yet supported.
   */
  public static <T> Dataset<T> initializeDataset(final SparkSession spark,
                                                 final LinkedHashMap<String, Object[]> commandList)
      throws OperationNotSupportedException {
    Object result = spark;

    for (Map.Entry<String, Object[]> command: commandList.entrySet()) {
      final String[] cmd = command.getKey().split("#");
      final String clazz = cmd[0];
      final String methodName = cmd[1];
      final Object[] args = command.getValue();
      final Class<?>[] argTypes = Stream.of(args).map(o -> o.getClass()).toArray(Class[]::new);

      if (!clazz.equals(SparkSession.class.getSimpleName())
          && !clazz.equals(DataFrameReader.class.getSimpleName())
          && !clazz.equals(Dataset.class.getSimpleName())) {
        throw new OperationNotSupportedException(cmd + " is not yet supported.");
      }

      try {
        final Method method = result.getClass().getDeclaredMethod(methodName, argTypes);
        result = method.invoke(result, args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return (Dataset<T>) result;
  }

  @Override
  public DataFrameReader read() {
    callSuperclassMethod(this, this);
    return new DataFrameReader(this);
  }

  @Override
  public Dataset<Row> baseRelationToDataFrame(final BaseRelation baseRelation) {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this, baseRelation));
  }

  @Override
  public Dataset<Row> createDataFrame(final JavaRDD<?> rdd, final Class<?> beanClass) {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this, rdd, beanClass));
  }

  @Override
  public Dataset<Row> createDataFrame(final JavaRDD<Row> rowRDD, final StructType schema) {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this, rowRDD, schema));
  }

  @Override
  public Dataset<Row> createDataFrame(final java.util.List<?> data, final Class<?> beanClass) {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this, data, beanClass));
  }

  @Override
  public Dataset<Row> createDataFrame(final java.util.List<Row> rows, final StructType schema) {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this, rows, schema));
  }

  @Override
  public Dataset<Row> createDataFrame(final RDD<?> rdd, final Class<?> beanClass) {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this, rdd, beanClass));
  }

  @Override
  public Dataset<Row> createDataFrame(final RDD<Row> rowRDD, final StructType schema) {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this, rowRDD, schema));
  }

  @Override
  public Dataset<Row> emptyDataFrame() {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this));
  }

  @Override
  public Dataset<Row> sql(final String sqlText) {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this, sqlText));
  }

  @Override
  public Dataset<Row> table(final String tableName) {
    return Dataset.from((org.apache.spark.sql.Dataset<Row>) callSuperclassMethod(this, this, tableName));
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
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("nemo_user"));
      return SparkSession.from(super.getOrCreate(), this.options);
    }
  }
}
