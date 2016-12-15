package dag.examples;

import dag.node.Do;

public class EmptyDo<I, O> extends Do<I, O> {
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
  public Iterable<O> compute(Iterable<I> input) {
    return null;
  }
}
