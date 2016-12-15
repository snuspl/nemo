package dag.node;

public abstract class Do<I, O> extends Node<I, O> {
  public abstract Iterable<O> compute(Iterable<I> input);
}
