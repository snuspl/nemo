package graph;

import java.util.concurrent.atomic.AtomicInteger;

public class IdManager {
  private static AtomicInteger nodeId = new AtomicInteger(1);
  private static AtomicInteger edgeId = new AtomicInteger(1);

  public static String newNodeId() {
    return "node" + nodeId.getAndIncrement();
  }
  public static String newEdgeId() {
    return "edge" + edgeId.getAndIncrement();
  }
}
