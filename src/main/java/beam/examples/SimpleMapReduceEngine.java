package beam.examples;

import dag.*;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;

public class SimpleMapReduceEngine {

  public static void executeDAG(final DAG dag) throws Exception {
    final List<Node> topoSorted = new LinkedList<>();
    DAG.doDFS(dag, (node -> topoSorted.add(0, node)), DAG.VisitOrder.Post);

    List<Iterable> data = null;
    for (final Node node : topoSorted) {
      if (node instanceof SourceNode) {
        final List<Source.Reader> readers = ((SourceNode) node).getSource().getReaders(10); // 10 Bytes
        data = new ArrayList<>(readers.size());
        for (final Source.Reader reader : readers) {
          data.add(reader.read());
        }
      } else if (node instanceof InternalNode) {
        if (dag.getInEdges(node).get().get(0).getType() == Edge.Type.M2M) {
          data = shuffle(data);
        }

        final Operator op = ((InternalNode) node).getOperator();
        final List<Iterable> newData = new ArrayList<>();
        for (Iterable iterable : data) {
          newData.add(op.compute(iterable));
        }
        data = newData;
      } else if (node instanceof SinkNode) {
        throw new UnsupportedOperationException();
      } else {
        throw new UnsupportedOperationException();
      }

      System.out.println("Data after " + node + ": " + data);
    }

    System.out.println("DAG Output");
    System.out.println(data);
  }

  private static List<Iterable> shuffle(final List<Iterable> data) {
    final HashMap<Integer, HashMap<Object, KV<Object, List>>> dstIdToCombined = new HashMap<>();
    final int numDest = 5;

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
