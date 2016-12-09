package graph.examples;

import graph.Operator;

public class EmptyOp<TInK, TInV, TOutK, TOutV> implements Operator<TInK, TInV, TOutK, TOutV> {
  private final String name;

  public EmptyOp(final String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }
}
