package dag.examples;

import dag.node.Do;

import java.util.Map;

public class EmptyDo<I, O, T> extends Do<I, O, T> {
  private final String name;

  public EmptyDo(final String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", name: ");
    sb.append(name);
    return sb.toString();
  }

  @Override
  public Iterable<O> compute(Iterable<I> input, Map<T, Object> broadcasted) {
    return null;
  }
}
