package edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.PartitioningProperty;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.Hash;

/**
 * Pass for initiating IREdge Partitioning ExecutionProperty with default values.
 */
public final class DefaultPartitioningPropertyPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DefaultPartitioningPropertyPass";

  public DefaultPartitioningPropertyPass() {
    super(ExecutionProperty.Key.Partitioning);
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex ->
      dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> irEdge.setProperty(PartitioningProperty.of(Hash.class))));
    return dag;
  }
}
