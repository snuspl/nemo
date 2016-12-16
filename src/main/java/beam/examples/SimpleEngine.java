package beam.examples;

import dag.*;
import dag.node.*;
import dag.node.Do;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SimpleEngine {

  public static void executeDAG(final DAG dag) throws Exception {
    final List<Node> topoSorted = new LinkedList<>();
    DAG.doDFS(dag, (node -> topoSorted.add(0, node)), DAG.VisitOrder.Post);

    // TODO: Use a hashmap: edgeId -> data
    final Map<String, List<Iterable>> edgeIdToData = new HashMap<>();
    final Map<Object, Object> broadcastTagToData = new HashMap<>();

    for (final Node node : topoSorted) {
      if (node instanceof Source) {
        final List<Source.Reader> readers = ((Source)node).getReaders(10); // 10 Bytes
        final List<Iterable> data = new ArrayList<>(readers.size());
        for (final Source.Reader reader : readers) {
          data.add(reader.read());
        }

        dag.getOutEdges(node).get().stream()
            // .filter(outEdge -> outEdge.getType() != Edge.Type.O2M)
            .map(outEdge -> outEdge.getId())
            .forEach(id -> edgeIdToData.put(id, data));
      } else if (node instanceof Do) {
        final Do op = (Do)node;
        final List<Iterable> data = new ArrayList<>();

        final Map<Object, Object> broadcasted = new HashMap<>();
        dag.getInEdges(node).get().stream()
            .filter(inEdge -> inEdge.getSrc() instanceof Broadcast)
            .map(inEdge -> ((Broadcast)inEdge.getSrc()).getTag())
            .forEach(tag -> broadcasted.put(tag, broadcastTagToData.get(tag)));

        dag.getInEdges(node).get().stream()
            .filter(inEdge -> !(inEdge.getSrc() instanceof Broadcast))
            .map(inEdge -> edgeIdToData.get(inEdge.getId()))
            .findFirst()
            .get()
            .forEach(iterable -> data.add(op.compute(iterable, broadcastTagToData)));

        System.out.println("DATA: " + data);

        edgeIdToData.put(dag.getOutEdges(node).get().iterator().next().getId(), data);
      } else if (node instanceof GroupByKey) {
        if (dag.getInEdges(node).get().get(0).getType() != Edge.Type.M2M) {
          throw new IllegalStateException();
        }
        final List<Iterable> data = shuffle(edgeIdToData.get(dag.getInEdges(node).get().iterator().next().getId()));
        edgeIdToData.put(dag.getOutEdges(node).get().iterator().next().getId(), data);
      } else if (node instanceof Broadcast) {
        if (dag.getInEdges(node).get().get(0).getType() != Edge.Type.O2M) {
          throw new IllegalStateException();
        }
        final Broadcast broadcast = (Broadcast)node;
        final List<Iterable> inEdgeData = edgeIdToData.get(dag.getInEdges(node).get().iterator().next().getId());
        final Object flattened = inEdgeData.stream()
            .flatMap(iterable -> StreamSupport.stream(iterable.spliterator(), false))
            .collect(Collectors.toList());

         broadcastTagToData.put(broadcast.getTag(), flattened);
      } else if (node instanceof Sink) {
        throw new UnsupportedOperationException();
      } else {
        throw new UnsupportedOperationException();
      }

      System.out.println("Edge Data after " + node.getId() + ": " + edgeIdToData);
      System.out.println("Broadcasted Data after " + node.getId() + ": " + broadcastTagToData);
    }
  }


  private static List<Iterable> shuffle(final List<Iterable> data) {
    final HashMap<Integer, HashMap<Object, KV<Object, List>>> dstIdToCombined = new HashMap<>();
    final int numDest = 3;

    data.forEach(iterable -> iterable.forEach(element -> {
      final KV kv = (KV) element;
      final int dstId = kv.getKey().hashCode() % numDest;
      dstIdToCombined.putIfAbsent(dstId, new HashMap<>());
      final HashMap<Object, KV<Object, List>> combined = dstIdToCombined.get(dstId);
      combined.putIfAbsent(kv.getKey(), KV.of(kv.getKey(), new ArrayList()));
      combined.get(kv.getKey()).getValue().add(kv.getValue());
    }));

    return dstIdToCombined.values().stream()
        .map(map -> map.values().stream().collect(Collectors.toList()))
        .collect(Collectors.toList());
  }
}
