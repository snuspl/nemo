package edu.snu.onyx.compiler.optimizer.pass.compiletime.composite;

import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.DefaultStagePartitioningPass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.ReviseInterStageEdgeDataStorePass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.ScheduleGroupPass;

import java.util.Arrays;

/**
 * A series of primitive passes that is applied commonly to all policies.
 * It is highly recommended to place reshaping passes before this pass,
 * and annotating passes after that and before this pass.
 */
public final class PrimitiveCompositePass extends CompositePass {
  public PrimitiveCompositePass() {
    super(Arrays.asList(
        new DefaultParallelismPass(), // annotating after reshaping passes, before stage partitioning
        new DefaultStagePartitioningPass(),
        new ReviseInterStageEdgeDataStorePass(), // after stage partitioning
        new ScheduleGroupPass()
    ));
  }
}
