package dag.node;

import util.IdManager;

import java.io.Serializable;
import java.util.HashMap;

public abstract class Node<I, O> implements Serializable {
  private final String id;
  private final HashMap<String, Object> attributes;

  public Node() {
    this.id = IdManager.newNodeId();
    this.attributes = new HashMap<>();
  }

  public String getId() {
    return id;
  }

  public HashMap<String, Object> getAttributes() {
    return attributes;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("class: ");
    sb.append(this.getClass().toString());
    sb.append(", id: ");
    sb.append(id);
    sb.append(", attributes: ");
    sb.append(attributes);
    return sb.toString();
  }
}
