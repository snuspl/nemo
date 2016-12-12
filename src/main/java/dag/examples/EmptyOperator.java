package dag.examples;

import dag.Operator;

public class EmptyOperator<I, O> implements Operator<I, O> {
  private final String name;

  public EmptyOperator(final String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public Iterable<O> compute(Iterable<I> input) {
    return null;
  }
}
