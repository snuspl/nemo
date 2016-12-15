package beam.examples;

import dag.*;
import dag.node.*;
import dag.node.Do;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;

public class SimpleMapReduceEngine {

  public static void executeDAG(final DAG dag) throws Exception {
    final List<Node> topoSorted = new LinkedList<>();
    DAG.doDFS(dag, (node -> topoSorted.add(0, node)), DAG.VisitOrder.Post);

    List<Iterable> data = null;
    for (final Node node : topoSorted) {
      if (node instanceof Source) {
        final List<Source.Reader> readers = ((Source)node).getReaders(10); // 10 Bytes
        data = new ArrayList<>(readers.size());
        for (final Source.Reader reader : readers) {
          data.add(reader.read());
        }
      } else if (node instanceof Do) {
        final Do op = (Do)node;
        final List<Iterable> newData = new ArrayList<>();
        for (Iterable iterable : data) {
          newData.add(op.compute(iterable));
        }
        data = newData;
      } else if (node instanceof GroupByKey) {
        if (dag.getInEdges(node).get().get(0).getType() != Edge.Type.M2M) {
          throw new IllegalStateException();
        }
        data = shuffle(data);
      } else if (node instanceof Sink) {
        throw new UnsupportedOperationException();
      } else {
        throw new UnsupportedOperationException();
      }

      System.out.println("Data after " + node + ": " + data);
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
