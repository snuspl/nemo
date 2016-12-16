package dag.node;

public abstract class Broadcast<I, O, T> extends Node<I, O> {
  // TODO: Get Type: Map/Iterable/ETC

  public abstract T getTag();
}
