package edu.snu.onyx.compiler.optimizer.policy;

import edu.snu.onyx.compiler.exception.CompileTimeOptimizationException;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.DefaultStagePartitioningPass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.ScheduleGroupPass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.composite.*;
import edu.snu.onyx.compiler.optimizer.pass.runtime.DataSkewRuntimePass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class PolicyBuilderTest {
  @Test
  public void testDisaggregationPolicy() {
    final Policy disaggregationPolicy = new PolicyBuilder()
        .registerCompileTimePass(new InitiationCompositePass())
        .registerCompileTimePass(new LoopOptimizationCompositePass())
        .registerCompileTimePass(new DisaggregationPass())
        .registerCompileTimePass(new DefaultStagePartitioningPass())
        .registerCompileTimePass(new ScheduleGroupPass())
        .build();
    assertEquals(33, disaggregationPolicy.getCompileTimePasses().size());
    assertEquals(0, disaggregationPolicy.getRuntimePasses().size());
  }

  @Test
  public void testPadoPolicy() {
    final Policy padoPolicy = new PolicyBuilder()
        .registerCompileTimePass(new InitiationCompositePass())
        .registerCompileTimePass(new LoopOptimizationCompositePass())
        .registerCompileTimePass(new PadoCompositePass())
        .registerCompileTimePass(new DefaultStagePartitioningPass())
        .registerCompileTimePass(new ScheduleGroupPass())
        .build();
    assertEquals(34, padoPolicy.getCompileTimePasses().size());
    assertEquals(0, padoPolicy.getRuntimePasses().size());
  }

  @Test
  public void testDataSkewPolicy() {
    final Policy dataSkewPolicy = new PolicyBuilder()
        .registerCompileTimePass(new InitiationCompositePass())
        .registerCompileTimePass(new LoopOptimizationCompositePass())
        .registerRuntimePass(new DataSkewRuntimePass(), new DataSkewCompositePass())
        .registerCompileTimePass(new DefaultStagePartitioningPass())
        .registerCompileTimePass(new ScheduleGroupPass())
        .build();
    assertEquals(41, dataSkewPolicy.getCompileTimePasses().size());
    assertEquals(1, dataSkewPolicy.getRuntimePasses().size());
  }

  @Test
  public void testShouldFailPolicy() {
    try {
      final Policy failPolicy = new PolicyBuilder()
          .registerCompileTimePass(new PadoCompositePass())
          .registerCompileTimePass(new DefaultStagePartitioningPass())
          .registerCompileTimePass(new ScheduleGroupPass())
          .build();
    } catch (Exception e) {
      assertTrue(e instanceof CompileTimeOptimizationException);
      assertTrue(e.getMessage().contains("Prerequisite ExecutionProperty hasn't been met"));
    }
  }


}
