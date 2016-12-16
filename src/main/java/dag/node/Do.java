package dag.node;

import java.util.Map;

public abstract class Do<I, O, T> extends Node<I, O> {
  public abstract Iterable<O> compute(Iterable<I> input, Map<T, Object> broadcasted);
}
