package edu.snu.onyx.compiler.optimizer;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.SourceVertex;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.ir.executionproperty.vertex.ParallelismProperty;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import edu.snu.onyx.compiler.optimizer.pass.runtime.RuntimePass;
import edu.snu.onyx.compiler.optimizer.policy.Policy;
import edu.snu.onyx.runtime.executor.datatransfer.communication.Broadcast;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

/**
 * A policy to test multi-parallelism. ONLY FOR TESTING!
 */
public class TestMultiParallelismPolicy implements Policy {
  private Policy testPolicy = new TestPolicy();

  @Override
  public List<CompileTimePass> getCompileTimePasses() {
    final List<CompileTimePass> list = new ArrayList<>();
    list.add(new DualParallelismPass());
    list.addAll(testPolicy.getCompileTimePasses());
    return list;
  }

  @Override
  public List<RuntimePass<?>> getRuntimePasses() {
    return new ArrayList<>();
  }

  private final class DualParallelismPass extends AnnotatingPass {
    public static final String SIMPLE_NAME = "DualParallelismPass";

    public DualParallelismPass() {
      super(ExecutionProperty.Key.Parallelism);
    }

    @Override
    public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
      // Propagate forward source parallelism
      dag.topologicalDo(vertex -> {
        try {
          final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex).stream()
              .filter(edge -> !Boolean.TRUE.equals(edge.isSideInput()))
              .collect(Collectors.toList());
          if (inEdges.isEmpty() && vertex instanceof SourceVertex) {
            final SourceVertex sourceVertex = (SourceVertex) vertex;
            vertex.setProperty(ParallelismProperty.of(sourceVertex.getReaders(2).size()));
          } else if (!inEdges.isEmpty()) {
            final OptionalInt parallelism = inEdges.stream()
                // No reason to propagate via Broadcast edges, as the data streams that will use the broadcasted data
                // as a sideInput will have their own number of parallelism
                .filter(edge -> !Broadcast.class.equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)))
                .mapToInt(edge -> edge.getSrc().getProperty(ExecutionProperty.Key.Parallelism))
                .max();
            if (parallelism.isPresent()) {
              vertex.setProperty(ParallelismProperty.of(parallelism.getAsInt()));
            }
          } else {
            throw new RuntimeException("There is a non-source vertex that doesn't have any inEdges other than SideInput");
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      return dag;
    }
  }
}
