package graph;

import java.util.HashMap;

public class Edge<K, V> {
  public enum Type {
    NtoN,
    OtoN,
    OtoO,
    NtoO
  }

  private final String id;
  private final HashMap<String, Object> attributes;
  private final Type type;
  private final Node<?, ?, K, V> src;
  private final Node<K, V, ?, ?> dst;

 public Edge(final Type type,
             final Node<?, ?, K, V> src,
             final Node<K, V, ?, ?> dst) {
    this.id = IdManager.newEdgeId();
    attributes = new HashMap<>(0);
    this.type = type;
    this.src = src;
    this.dst = dst;
  }

  Edge setAttr(final String key, final Object val) {
    attributes.put(key, val);
    return this;
  }

  Object getAttr(final String key) {
    return attributes.get(key);
  }

  public Type getType() {
    return type;
  }

  public Node<?, ?, K, V> getSrc() {
    return src;
  }

  public Node<K, V, ?, ?> getDst() {
    return dst;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("id: ");
    sb.append(id);
    sb.append(", attributes: ");
    sb.append(attributes);
    sb.append(", type: ");
    sb.append(type);
    return sb.toString();
  }
}
