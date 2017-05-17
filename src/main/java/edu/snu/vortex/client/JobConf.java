package edu.snu.vortex.client;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Vortex Job Configurations.
 */
public final class JobConf extends ConfigurationModuleBuilder {

  //////////////////////////////// User Configurations

  /**
   * Job id.
   */
  @NamedParameter(doc = "Job id", short_name = "job_id")
  public final class JobId implements Name<String> {
  }

  /**
   * User Main Class Name.
   */
  @NamedParameter(doc = "User Main Class Name", short_name = "user_main")
  public final class UserMainClass implements Name<String> {
  }

  /**
   * User Main Arguments.
   */
  @NamedParameter(doc = "User Main Arguments", short_name = "user_args")
  public final class UserMainArguments implements Name<String> {
  }

  /**
   * Directory to store JSON representation of intermediate DAGs.
   */
  @NamedParameter(doc = "Directory to store intermediate DAGs", short_name = "dag_dir", default_value = "./target/dag")
  public final class DAGDirectory implements Name<String> {
  }

  static final RequiredParameter<String> JOB_ID = new RequiredParameter<>();
  static final RequiredParameter<String> USER_MAIN_CLASS = new RequiredParameter<>();
  static final RequiredParameter<String> USER_MAIN_ARGS = new RequiredParameter<>();
  static final OptionalParameter<String> DAG_DIRECTORY = new OptionalParameter<>();

  //////////////////////////////// UserApplicationRunner Configurations

  /**
   * Name of the optimization policy.
   */
  @NamedParameter(doc = "Name of the optimization policy", short_name = "optimization_policy", default_value = "none")
  public final class OptimizationPolicy implements Name<String> {
  }

  static final OptionalParameter<String> OPTIMIZATION_POLICY = new OptionalParameter<>();

  //////////////////////////////// Runtime Configurations

  /**
   * Vortex driver memory.
   */
  @NamedParameter(doc = "Vortex driver memory", short_name = "driver_mem")
  public final class DriverMem implements Name<Integer> {
  }

  /**
   * Number of vortex executors.
   */
  @NamedParameter(doc = "Number of vortex executors", short_name = "executor_num")
  public final class ExecutorNum implements Name<Integer> {
  }

  /**
   * Vortex executor memory.
   */
  @NamedParameter(doc = "Vortex executor memory", short_name = "executor_mem")
  public final class ExecutorMem implements Name<Integer> {
  }

  /**
   * Vortex executor cores.
   */
  @NamedParameter(doc = "Vortex executor cores", short_name = "executor_cores")
  public final class ExecutorCores implements Name<Integer> {
  }

  /**
   * VortexExecutor capacity.
   */
  @NamedParameter(doc = "VortexExecutor capacity", short_name = "executor_capacity")
  public final class ExecutorCapacity implements Name<Integer> {
  }

  /**
   * Scheduler timeout in ms.
   */
  @NamedParameter(doc = "Scheduler timeout in ms", short_name = "scheduler_timeout_ms")
  public final class SchedulerTimeoutMs implements Name<Integer> {
  }


  static final RequiredParameter<Integer> DRIVER_MEM = new RequiredParameter<>();
  static final RequiredParameter<Integer> EXECUTOR_NUM = new RequiredParameter<>();
  static final RequiredParameter<Integer> EXECUTOR_MEM = new RequiredParameter<>();
  static final RequiredParameter<Integer> EXECUTOR_CORES = new RequiredParameter<>();
  static final RequiredParameter<Integer> EXECUTOR_CAPACITY = new RequiredParameter<>();
  static final RequiredParameter<Integer> SCHEDULER_TIMEOUT_MS = new RequiredParameter<>();

  //////////////////////////////// Configuration Module

}
