package dag;

import java.util.HashMap;

public class Edge<I, O> {
  public enum Type {
    M2M,
    O2M,
    O2O,
    N2N
  }

  private final String id;
  private final HashMap<String, Object> attributes;
  private final Type type;
  private final Node<?, I> src;
  private final Node<O, ?> dst;

  Edge(final Type type,
       final Node<?, I> src,
       final Node<O, ?> dst) {
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

  public Node<?, I> getSrc() {
    return src;
  }

  public Node<O, ?> getDst() {
    return dst;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("id: ");
    sb.append(id);
    sb.append(", src: ");
    sb.append(src.getId());
    sb.append(", dst: ");
    sb.append(dst.getId());
    sb.append(", attributes: ");
    sb.append(attributes);
    sb.append(", type: ");
    sb.append(type);
    return sb.toString();
  }
}
