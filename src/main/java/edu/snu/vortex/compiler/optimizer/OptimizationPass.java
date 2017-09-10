package edu.snu.vortex.compiler.optimizer;

import edu.snu.vortex.compiler.optimizer.passes.*;
import edu.snu.vortex.compiler.optimizer.passes.optimization.CommonSubexpressionEliminationPass;
import edu.snu.vortex.compiler.optimizer.passes.optimization.LoopOptimizations;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class OptimizationPass {
  /**
   * A private constructor.
   */
  private OptimizationPass() {
  }

  /**
   * A list of names of pre-defined passes.
   */
  public static final String COMMON_SUBEXPRESSION_ELIMINATION = "common_subexpression_elimination";
  public static final String LOOP_FUSION = "loop_fusion";
  public static final String LOOP_INVARIANT_CODE_MOTION = "loop_invariant_code_motion";
  public static final String PARALLELISM = "parallelism";
  public static final String PADO_VERTEX = "pado_vertex";
  public static final String PADO_EDGE = "pado_edge";
  public static final String DISAGGREGATION = "disaggregation";
  public static final String IFILE = "ifile";
  public static final String DATA_SKEW = "data_skew";
  public static final String LOOP_GROUPING = "loop_grouping";
  public static final String LOOP_UNROLLING = "loop_unrolling";
  public static final String DEFAULT_STAGE_PARTITIONING = "default_stage_partitioning";
  public static final String SCHEDULE_GROUP = "schedule_group";

  /**
   * A HashMap to convert string names to passes. String names are received as arguments.
   * You should be able to see the full list of passes here.
   */
  private static final Map<String, StaticOptimizationPass> PASSES = new HashMap<>();

  /**
   * A list of pre-defined passes.
   */
  static {
    // Optimization
    PASSES.put(COMMON_SUBEXPRESSION_ELIMINATION, new CommonSubexpressionEliminationPass());
    PASSES.put(LOOP_FUSION, new LoopOptimizations.LoopFusionPass());
    PASSES.put(LOOP_INVARIANT_CODE_MOTION, new LoopOptimizations.LoopInvariantCodeMotionPass());

    PASSES.put(PARALLELISM, new ParallelismPass());
    PASSES.put(PADO_VERTEX, new PadoVertexPass());
    PASSES.put(PADO_EDGE, new PadoEdgePass());
    PASSES.put(DISAGGREGATION, new DisaggregationPass());
    PASSES.put(IFILE, new IFilePass());
    PASSES.put(DATA_SKEW, new DataSkewPass());
    PASSES.put(LOOP_GROUPING, new LoopGroupingPass());
    PASSES.put(LOOP_UNROLLING, new LoopUnrollingPass());
    PASSES.put(DEFAULT_STAGE_PARTITIONING, new DefaultStagePartitioningPass());
    PASSES.put(SCHEDULE_GROUP, new ScheduleGroupPass());
  }

  /**
   * @param name name of the pass.
   * @return the pass with the given name.
   */
  public static StaticOptimizationPass getPassCalled(final String name) {
    return PASSES.get(name);
  }

  /**
   * @return a set of pass names that are already defined.
   */
  public static Set<String> getPassNames() {
    return PASSES.keySet();
  }

  /**
   * A public method for registering new passes to the list.
   * @param passName the name of the pass to be newly added.
   * @param pass the pass to be newly added.
   */
  public static void registerPass(final String passName, final StaticOptimizationPass pass) {
    if (getPassNames().contains(passName)) {
      throw new RuntimeException("A pass called " + passName + " already exists. Please try another name.");
    }
    PASSES.put(passName, pass);
  }
}
