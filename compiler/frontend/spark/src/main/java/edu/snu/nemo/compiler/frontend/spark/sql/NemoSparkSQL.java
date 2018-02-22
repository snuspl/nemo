package edu.snu.nemo.compiler.frontend.spark.sql;

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
}
