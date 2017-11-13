package edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.ir.executionproperty.edge.DataStoreProperty;
import edu.snu.onyx.runtime.executor.data.stores.LocalFileStore;
import edu.snu.onyx.runtime.executor.data.stores.MemoryStore;
import edu.snu.onyx.runtime.executor.datatransfer.communication.OneToOne;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Default edge data store pass.
 */
public class DefaultEdgeDataStorePass extends AnnotatingPass {
  public DefaultEdgeDataStorePass() {
    super(ExecutionProperty.Key.DataStore, Stream.of(
        ExecutionProperty.Key.StageId,
        ExecutionProperty.Key.DataCommunicationPattern
    ).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (OneToOne.class.equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
            if (!edge.getSrc().getProperty(ExecutionProperty.Key.StageId)
                .equals(edge.getDst().getProperty(ExecutionProperty.Key.StageId))) {
              edge.setProperty(DataStoreProperty.of(LocalFileStore.class));
            } else {
              edge.setProperty(DataStoreProperty.of(MemoryStore.class));
            }
          } else {
            edge.setProperty(DataStoreProperty.of(LocalFileStore.class));
          }
        });
      }
    });
    return dag;
  }
}
