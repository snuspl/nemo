package edu.snu.nemo.compiler.frontend.spark.sql;

/**
 * Common interface for Nemo Spark classes.
 */
public interface NemoSparkSQL {
  /**
   * @return the userTriggered flag.
   */
  boolean isUserTriggered();

  /**
   * Set the userTriggered flag.
   * @param bool boolean to set the flag to.
   */
  void setUserTriggered(boolean bool);

  /**
   * @return the sparkSession of the instance.
   */
  SparkSession sparkSession();

  /**
   * A method to distinguish user-called functions from internal calls.
   * @param args arguments of the method
   * @return whether or not this function has been called by the user.
   */
  default boolean initializeFunction(final Object... args) {
    if (!isUserTriggered()) {
      return false;
    }

    final String className = Thread.currentThread().getStackTrace()[2].getClassName();
    final String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();

    sparkSession().appendCommand(className + "#" + methodName, args);
    setUserTriggered(false);

    return true;
  }
}
