package edu.snu.vortex.compiler.optimizer;

import edu.snu.vortex.compiler.optimizer.passes.*;

import java.util.*;

/**
 * A class for maintaining Optimization Policies, which are each composed of a list of Optimization Passes.
 */
public final class OptimizationPolicy {
  /**
   * A private constructor.
   */
  private OptimizationPolicy() {
  }

  /**
   * A list of names of pre-defined policies.
   */
  public static final String DEFAULT = "default";
  public static final String PADO = "pado";
  public static final String DISAGGREGATION = "disaggregation";
  public static final String DATASKEW = "dataskew";
  public static final String TEST = "test";

  /**
   * A HashMap to match each of instantiation policies with a combination of instantiation passes.
   * Each policies are run in the order with which they are defined.
   */
  private static final Map<String, List<StaticOptimizationPass>> POLICIES = new HashMap<>();

  /**
   * A list of pre-defined policies.
   */
  static {
    registerPolicy(DEFAULT,
        Arrays.asList(
            OptimizationPass.getPassCalled(OptimizationPass.PARALLELISM), // Provides parallelism information.
            OptimizationPass.getPassCalled(OptimizationPass.DEFAULT_STAGE_PARTITIONING),
            OptimizationPass.getPassCalled(OptimizationPass.SCHEDULE_GROUP)
        ));
    registerPolicy(PADO,
        Arrays.asList(
            OptimizationPass.getPassCalled(OptimizationPass.PARALLELISM), // Provides parallelism information.
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_GROUPING),
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_FUSION),
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_INVARIANT_CODE_MOTION),
            // Groups then unrolls loops. TODO #162: remove unrolling pt.
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_UNROLLING),
            // Processes vertices and edges with Pado algorithm.
            OptimizationPass.getPassCalled(OptimizationPass.PADO_VERTEX),
            OptimizationPass.getPassCalled(OptimizationPass.PADO_EDGE),
            OptimizationPass.getPassCalled(OptimizationPass.DEFAULT_STAGE_PARTITIONING),
            OptimizationPass.getPassCalled(OptimizationPass.SCHEDULE_GROUP)
        ));
    registerPolicy(DISAGGREGATION,
        Arrays.asList(
            OptimizationPass.getPassCalled(OptimizationPass.PARALLELISM), // Provides parallelism information.
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_GROUPING),
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_FUSION),
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_INVARIANT_CODE_MOTION),
            // Groups then unrolls loops. TODO #162: remove unrolling pt.
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_UNROLLING),
            OptimizationPass.getPassCalled(OptimizationPass.DISAGGREGATION),
            OptimizationPass.getPassCalled(OptimizationPass.IFILE),
            OptimizationPass.getPassCalled(OptimizationPass.DEFAULT_STAGE_PARTITIONING),
            OptimizationPass.getPassCalled(OptimizationPass.SCHEDULE_GROUP)
        ));
    registerPolicy(DATASKEW,
        Arrays.asList(
            OptimizationPass.getPassCalled(OptimizationPass.PARALLELISM), // Provides parallelism information.
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_GROUPING),
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_FUSION),
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_INVARIANT_CODE_MOTION),
            // Groups then unrolls loops. TODO #162: remove unrolling pt.
            OptimizationPass.getPassCalled(OptimizationPass.LOOP_UNROLLING),
            OptimizationPass.getPassCalled(OptimizationPass.DATA_SKEW),
            OptimizationPass.getPassCalled(OptimizationPass.DEFAULT_STAGE_PARTITIONING),
            OptimizationPass.getPassCalled(OptimizationPass.SCHEDULE_GROUP)
        ));
    registerPolicy(TEST, // Simply build stages for tests
        Arrays.asList(
            OptimizationPass.getPassCalled(OptimizationPass.DEFAULT_STAGE_PARTITIONING),
            OptimizationPass.getPassCalled(OptimizationPass.SCHEDULE_GROUP)
        ));
  }

  /**
   * @param name name of the policy.
   * @return the policy with the given name.
   */
  public static List<StaticOptimizationPass> getPolicyCalled(final String name) {
    return POLICIES.get(name);
  }

  /**
   * @return a set of policy names that are defined.
   */
  public static Set<String> getPolicyNames() {
    return POLICIES.keySet();
  }

  /**
   * A public method for registering new policies to the list.
   * @param name the name of the policy that is to be newly registered.
   * @param passes content of the new policy.
   */
  public static void registerPolicy(final String name, final List<StaticOptimizationPass> passes) {
    if (getPolicyNames().contains(name)) {
      throw new RuntimeException("A policy called " + name + " already exists. Please try another name");
    }
    POLICIES.put(name, passes);
  }
}
